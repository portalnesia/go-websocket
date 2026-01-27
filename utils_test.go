package websocket

import (
	"strings"
	"sync"
	"testing"
)

func TestGenerateId(t *testing.T) {
	// Test that generateId returns non-empty string
	id := generateId()
	if id == "" {
		t.Error("generateId() returned empty string")
	}

	// Test that generateId returns valid ULID format (26 characters)
	if len(id) != 26 {
		t.Errorf("generateId() returned invalid length, got %d, want 26", len(id))
	}

	// Test that generateId returns unique IDs
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := generateId()
		if ids[id] {
			t.Errorf("generateId() returned duplicate ID: %s", id)
		}
		ids[id] = true
	}
}

func TestGenerateIdConcurrency(t *testing.T) {
	// Test concurrent ID generation
	const goroutines = 100
	const idsPerGoroutine = 100

	ids := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id := generateId()
				if _, loaded := ids.LoadOrStore(id, true); loaded {
					t.Errorf("generateId() returned duplicate ID in concurrent test: %s", id)
				}
			}
		}()
	}

	wg.Wait()
}

func TestCryptoReader(t *testing.T) {
	buf := make([]byte, 16)
	reader := &cryptoReader{buf: buf}

	// Test Read
	p := make([]byte, 8)
	n, err := reader.Read(p)
	if err != nil {
		t.Errorf("cryptoReader.Read() error = %v", err)
	}
	if n != 8 {
		t.Errorf("cryptoReader.Read() n = %d, want 8", n)
	}

	// Test Read wraps around
	p2 := make([]byte, 16)
	n, err = reader.Read(p2)
	if err != nil {
		t.Errorf("cryptoReader.Read() error = %v", err)
	}
	if n != 8 {
		t.Errorf("cryptoReader.Read() n = %d, want 8", n)
	}
	if reader.pos != 0 {
		t.Errorf("cryptoReader.pos = %d, want 0 after wrap", reader.pos)
	}
}

func TestCopyMap(t *testing.T) {
	t.Run("nil map", func(t *testing.T) {
		var nilMap map[string]int
		copied := copyMap(nilMap)
		if copied != nil {
			t.Error("copyMap(nil) should return nil")
		}
	})

	t.Run("empty map", func(t *testing.T) {
		emptyMap := make(map[string]int)
		copied := copyMap(emptyMap)
		if copied == nil {
			t.Error("copyMap(empty) should not return nil")
		}
		if len(copied) != 0 {
			t.Errorf("copyMap(empty) length = %d, want 0", len(copied))
		}
	})

	t.Run("non-empty map", func(t *testing.T) {
		original := map[string]int{
			"one":   1,
			"two":   2,
			"three": 3,
		}
		copied := copyMap(original)

		// Check all keys and values are copied
		if len(copied) != len(original) {
			t.Errorf("copied map length = %d, want %d", len(copied), len(original))
		}

		for k, v := range original {
			if copied[k] != v {
				t.Errorf("copied[%s] = %d, want %d", k, copied[k], v)
			}
		}

		// Verify it's a deep copy
		original["four"] = 4
		if _, exists := copied["four"]; exists {
			t.Error("copyMap() should create a deep copy, not a reference")
		}
	})

	t.Run("with different types", func(t *testing.T) {
		originalString := map[string]string{
			"name": "test",
			"type": "string",
		}
		copiedString := copyMap(originalString)

		if len(copiedString) != len(originalString) {
			t.Errorf("copied string map length = %d, want %d", len(copiedString), len(originalString))
		}

		for k, v := range originalString {
			if copiedString[k] != v {
				t.Errorf("copiedString[%s] = %s, want %s", k, copiedString[k], v)
			}
		}
	})
}

func BenchmarkGenerateId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = generateId()
	}
}

func BenchmarkGenerateIdParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = generateId()
		}
	})
}

func BenchmarkCopyMap(b *testing.B) {
	original := map[string]int{
		"one":   1,
		"two":   2,
		"three": 3,
		"four":  4,
		"five":  5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = copyMap(original)
	}
}

func TestGenerateIdFormat(t *testing.T) {
	// Test that generated IDs contain only valid ULID characters
	validChars := "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

	for i := 0; i < 100; i++ {
		id := generateId()
		for _, char := range id {
			if !strings.ContainsRune(validChars, char) {
				t.Errorf("generateId() returned invalid character '%c' in ID: %s", char, id)
			}
		}
	}
}
