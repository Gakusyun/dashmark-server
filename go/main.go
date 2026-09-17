// DashMark 云同步 · Go 自部署实现（SQLite 存储）
//
// 只存储加密后的 gzip 数据包，接口契约见仓库根 README.md。
// 纯 Go SQLite 驱动，无 CGO，编译出单个静态二进制，内存占用极低。
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
	"database/sql"
)

const maxBody = 1 << 20 // 1 MiB

// ==================== 惰性清理（垃圾回收） ====================

const (
	cleanInterval    = 24 * time.Hour      // 清理冷却：24h 内不重复清理，减少写消耗
	expireAfter      = 30 * 24 * time.Hour // 过期阈值：1 个月未访问即删除
	maxRows          = 50                  // 最大保留条数，超出淘汰最久未访问
	accessRefreshGap = 20 * time.Hour      // 访问刷新冷却：20h 内重复读取不更新 last_accessed
)

// ==================== 内存限流（尽力而为） ====================

const (
	rateWindow   = time.Minute
	rateReadMax  = 60
	rateWriteMax = 10
)

type rateEntry struct {
	count int
	reset time.Time
}

type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*rateEntry
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{buckets: make(map[string]*rateEntry)}
}

func (r *rateLimiter) allow(ip string, isWrite bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buckets) > 10_000 {
		r.buckets = make(map[string]*rateEntry)
	}
	now := time.Now()
	e, ok := r.buckets[ip]
	limit := rateReadMax
	if isWrite {
		limit = rateWriteMax
	}
	if !ok || now.After(e.reset) {
		r.buckets[ip] = &rateEntry{count: 1, reset: now.Add(rateWindow)}
		return true
	}
	if e.count >= limit {
		return false
	}
	e.count++
	return true
}

// ==================== 存储 ====================

type store struct {
	db *sql.DB
}

func openStore(path string) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// SQLite 单写者：限制连接数，进一步压低内存占用
	db.SetMaxOpenConns(1)
	schema := `
	CREATE TABLE IF NOT EXISTS sync_data (
		id            TEXT PRIMARY KEY,
		verifier      TEXT NOT NULL,
		payload       BLOB NOT NULL,
		payload_hash  TEXT NOT NULL,
		updated_at    INTEGER NOT NULL,
		last_accessed INTEGER NOT NULL DEFAULT 0
	);
	CREATE TABLE IF NOT EXISTS job_meta (
		key           TEXT PRIMARY KEY,
		last_clean_at INTEGER NOT NULL
	);
	INSERT OR IGNORE INTO job_meta(key, last_clean_at) VALUES('asset_clean', 0);`
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	// 老库迁移：补 last_accessed 列并用 updated_at 回填，避免存量数据被误判过期
	if _, err := db.Exec(
		`ALTER TABLE sync_data ADD COLUMN last_accessed INTEGER NOT NULL DEFAULT 0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return nil, err
		}
	}
	if _, err := db.Exec(
		`UPDATE sync_data SET last_accessed = updated_at WHERE last_accessed = 0`); err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

// lazyClean 惰性清理：每次请求进入时尝试执行，24h 冷却期内直接跳过。
// 任何失败都不影响业务请求。
func (s *store) lazyClean() {
	var lastClean int64
	err := s.db.QueryRow(`SELECT last_clean_at FROM job_meta WHERE key = 'asset_clean'`).Scan(&lastClean)
	if err != nil {
		return // 表或行异常，下次请求再试
	}
	now := time.Now()
	if now.UnixMilli()-lastClean < cleanInterval.Milliseconds() {
		return // 冷却期内，跳过
	}
	func() {
		defer func() { _ = recover() }()
		tx, err := s.db.Begin()
		if err != nil {
			return
		}
		defer tx.Rollback() // 提交后 defer 的 Rollback 会静默失败，仅错误路径生效，无副作用
		// ① 删除超过 1 个月未访问的记录
		if _, err := tx.Exec(`DELETE FROM sync_data WHERE last_accessed < ?`,
			now.Add(-expireAfter).UnixMilli()); err != nil {
			return
		}
		// ② 记录总数超过上限时，淘汰最久未访问的条目
		if _, err := tx.Exec(`DELETE FROM sync_data WHERE id NOT IN
			(SELECT id FROM sync_data ORDER BY last_accessed DESC LIMIT ?)`, maxRows); err != nil {
			return
		}
		if _, err := tx.Exec(`UPDATE job_meta SET last_clean_at = ? WHERE key = 'asset_clean'`,
			now.UnixMilli()); err != nil {
			return
		}
		_ = tx.Commit()
	}()
}

// touchAccess 访问时间惰性更新：20h 内重复读取不写库，减少写消耗
func (s *store) touchAccess(id string, lastAccessed int64) {
	if time.Now().UnixMilli()-lastAccessed <= accessRefreshGap.Milliseconds() {
		return
	}
	_, _ = s.db.Exec(`UPDATE sync_data SET last_accessed = ? WHERE id = ?`, time.Now().UnixMilli(), id)
}

type row struct {
	verifier     string
	payload      []byte
	payloadHash  string
	updatedAt    int64
	lastAccessed int64
}

func (s *store) get(id string) (*row, error) {
	var r row
	err := s.db.QueryRow(
		`SELECT verifier, payload, payload_hash, updated_at, last_accessed FROM sync_data WHERE id = ?`, id,
	).Scan(&r.verifier, &r.payload, &r.payloadHash, &r.updatedAt, &r.lastAccessed)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &r, err
}

func (s *store) getMeta(id string) (int64, error) {
	var updatedAt int64
	err := s.db.QueryRow(`SELECT updated_at FROM sync_data WHERE id = ?`, id).Scan(&updatedAt)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return updatedAt, err
}

func (s *store) put(id, verifier string, payload []byte, payloadHash string) (int64, error) {
	now := time.Now().UnixMilli()
	_, err := s.db.Exec(
		`INSERT INTO sync_data (id, verifier, payload, payload_hash, updated_at, last_accessed)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   verifier = excluded.verifier,
		   payload = excluded.payload,
		   payload_hash = excluded.payload_hash,
		   updated_at = excluded.updated_at,
		   last_accessed = excluded.last_accessed`,
		id, verifier, payload, payloadHash, now, now,
	)
	return now, err
}

// ==================== HTTP 处理 ====================

type server struct {
	store    *store
	limiter  *rateLimiter
}

func (s *server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) clientIP(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return strings.TrimSpace(strings.Split(ip, ",")[0])
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}

func (s *server) handleSync(w http.ResponseWriter, r *http.Request) {
	// 路径：/sync/{id} 或 /sync/{id}/meta
	parts := strings.TrimPrefix(r.URL.Path, "/sync/")
	if parts == r.URL.Path {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	isMeta := false
	id := parts
	if strings.HasSuffix(id, "/meta") {
		isMeta = true
		id = strings.TrimSuffix(id, "/meta")
	}
	if id == "" || strings.Contains(id, "/") {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}

	ip := s.clientIP(r)
	isWrite := r.Method == http.MethodPut
	if !s.limiter.allow(ip, isWrite) {
		s.writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
		return
	}

	switch {
	case r.Method == http.MethodGet && isMeta:
		s.handleMeta(w, r, id)
	case r.Method == http.MethodGet:
		s.handleGet(w, r, id)
	case r.Method == http.MethodPut:
		s.handlePut(w, r, id)
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *server) handleMeta(w http.ResponseWriter, r *http.Request, id string) {
	s.store.lazyClean()
	updatedAt, err := s.store.getMeta(id)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database error"})
		return
	}
	if updatedAt == 0 {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]int64{"updatedAt": updatedAt})
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request, id string) {
	s.store.lazyClean()
	row, err := s.store.get(id)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database error"})
		return
	}
	if row == nil {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if r.Header.Get("X-Dashmark-Verifier") != row.verifier {
		s.writeJSON(w, http.StatusForbidden, map[string]string{"error": "verifier mismatch"})
		return
	}
	// 访问时间惰性更新：20h 内重复读取不写库，减少写消耗（异步，不阻塞响应）
	go s.store.touchAccess(id, row.lastAccessed)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Updated-At", strconv.FormatInt(row.updatedAt, 10))
	_, _ = w.Write(row.payload)
}

func payloadFingerprint(r *http.Request, body []byte) string {
	// 优先使用客户端声明的明文哈希（密文带随机 IV，每次不同，
	// 对密文做哈希无法判定内容一致）；缺失时退化为对请求体做哈希
	if h := r.Header.Get("X-Dashmark-Hash"); isHex64(h) {
		return h
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request, id string) {
	s.store.lazyClean()
	verifier := r.Header.Get("X-Dashmark-Verifier")
	if id == "" || verifier == "" {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing id or verifier"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body failed"})
		return
	}
	if len(body) == 0 {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty body"})
		return
	}
	if len(body) > maxBody {
		s.writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "too large"})
		return
	}

	payloadHash := payloadFingerprint(r, body)

	existing, err := s.store.get(id)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database error"})
		return
	}
	if existing != nil {
		if existing.verifier != verifier {
			s.writeJSON(w, http.StatusForbidden, map[string]string{"error": "id taken"})
			return
		}
		if existing.payloadHash == payloadHash {
			s.writeJSON(w, http.StatusOK, map[string]any{
				"updatedAt": existing.updatedAt,
				"unchanged": true,
			})
			return
		}
	}

	updatedAt, err := s.store.put(id, verifier, body, payloadHash)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database error"})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]int64{"updatedAt": updatedAt})
}

// ==================== 入口 ====================

func main() {
	addr := flag.String("addr", ":8787", "监听地址")
	dbPath := flag.String("db", "sync.db", "SQLite 数据库文件路径")
	flag.Parse()

	st, err := openStore(*dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}

	srv := &server{store: st, limiter: newRateLimiter()}
	mux := http.NewServeMux()
	mux.HandleFunc("/sync/", srv.handleSync)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		srv.writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	})

	fmt.Printf("DashMark 云同步服务已启动：http://localhost%s\n", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("服务退出: %v", err)
	}
}
