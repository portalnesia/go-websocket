# AGENTS.md — go-websocket

Module `go.portalnesia.com/websocket` (Go 1.25). Library websocket ala socket.io: namespace, event,
room, multi-node via Redis. Klien cukup `WebSocket` native + JSON `{event, data}`. Tidak ada SDK
klien.

## Peta

| File | Isi |
|---|---|
| `server.go` | `Server`, handler `net/http` (coder/websocket), origin check, `Of(namespace)` |
| `server_fiber.go`, `ws_fiber.go` | Integrasi Fiber v2 (`gofiber/websocket/v2`) |
| `ws_conn.go`, `ws_coder.go` | Abstraksi `WSConn` + wrapper coder |
| `namespace.go` | Registry koneksi/room, emit, read/write loop, `closeConn` |
| `context.go` | `ConnContext`: request, `Local`, emit, join/leave room |
| `room.go`, `type.go` | `Room`, `Message`, `BroadcastData`, `Adapter` |
| `adapter/redis/` | Adapter Redis pub/sub (satu channel per namespace) |
| `logger.go`, `logger/*` | Interface `Logger` + adapter zerolog/zap/logrus |
| `examples/` | Contoh program (`main` terpisah) |

## Alur

- **Koneksi**: upgrade → origin check → namespace dari `?namespace=` → `OnConnect` (error = tolak) →
  goroutine read loop + write loop. Read loop meneruskan event ke handler `On`.
- **Emit**: kirim ke koneksi lokal (goroutine, `select` non-blocking ke buffer `cw.send`). Bila
  adapter ada, payload yang sama di-publish async. Node lain menerimanya lewat subscribe, dan pesan
  dari node sendiri diabaikan (`FromServerID`).
- **Room** hanya ada di memori node. Membership tidak disinkronkan antar node. Emit ke room
  disebar lewat adapter.
- **Pub/sub at-most-once**: pesan selama koneksi/adapter terputus hilang. Library tidak menyimpan
  maupun mengirim ulang.

## Aturan

1. **Git read-only**: boleh `status`, `diff`, `log`, `show`. Dilarang `commit`, `push`, `tag`, `branch`, `stash`, `reset`, `checkout`.
2. **API publik hanya bertambah**: jangan ubah/hapus signature exported. Method baru di interface exported (`WSConn`, `Adapter`) wajib diterapkan di semua implementasi + mock test.
3. **Wire format Redis** (`BroadcastData`): setelah ada rilis, perubahan format tidak boleh membuat node versi lama mengirim ke audiens lebih luas.
4. **Invariant concurrency** (`namespace.go`):
   - kirim ke `cw.send` hanya saat memegang `n.mu.RLock`, lewat `select` non-blocking;
   - `close(cw.send)` hanya di `closeConn` via `cw.once`, setelah conn dihapus dari map di bawah `n.mu.Lock`;
   - jangan pegang `n.mu` dan `n.roomsMu` bersamaan;
   - setiap goroutine yang menjalankan handler pengguna/adapter wajib `recover`;
   - setiap goroutine per-koneksi wajib berhenti saat `ctx.done` tertutup.
5. **Jalur emit non-blocking**: `Emit*`/`Close*` tidak boleh menunggu jaringan. Publish adapter tetap async + semaphore.
6. **Dependency baru dilarang** tanpa persetujuan pemilik.
7. **Log** lewat `GetLogger()`. Dilarang `fmt.Print*`/`log.*` langsung. Dilarang log isi pesan, token, atau query string.
8. **Nilai default** = konstanta exported (`Default…`), ditulis juga di README.
9. API publik bertambah → README + `examples/` diperbarui di perubahan yang sama.

## Test & verifikasi

- TDD: test RED terhadap kode yang belum disentuh → implementasi minimal → GREEN.
- Satu file test per file sumber (`x_test.go`). Dilarang `*_extra_test.go`.
- Dilarang jaringan eksternal. Pakai `WSConn` palsu, `httptest`, `miniredis` (contoh ada di
  `server_test.go`, `adapter/redis/redis_test.go`).
- Timing test memakai durasi milidetik lewat setter. Dilarang `time.Sleep` dalam hitungan detik.
- Gate berurutan. Simpan output utuh ke file, jangan di-pipe ke `tail`/`grep`:
  1. `gofmt -l .` → kosong
  2. `go vet ./...`
  3. `go test -race -count=1 ./...`
- `-race` butuh cgo (gcc). Bila tidak tersedia, **berhenti dan laporkan**. Jangan menjalankan test
  tanpa `-race` diam-diam.
