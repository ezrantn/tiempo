package tiempo

import "testing"

func TestWithMaxWALSizePositiveValueUpdatesMaxWALSize(t *testing.T) {
	db := &TiempoDB{}

	expectedSize := int64(1024 * 1024)
	opt := WithMaxWALSize(expectedSize)
	opt(db)

	if db.maxWALSize != expectedSize {
		t.Errorf("Expected maxWALSize to be %d, got %d", expectedSize, db.maxWALSize)
	}
}

func TestWithMaxWALSizeZeroValueDoesNotUpdateMaxWALSize(t *testing.T) {
	db := &TiempoDB{}

	initialSize := db.maxWALSize
	opt := WithMaxWALSize(0)
	opt(db)

	if db.maxWALSize != initialSize {
		t.Errorf("Expected maxWALSize to remain %d, got %d", initialSize, db.maxWALSize)
	}
}

func TestWithMaxWALSizeMultipleUpdates(t *testing.T) {
	db := &TiempoDB{}

	firstSize := int64(1024 * 1024)
	secondSize := int64(2048 * 2048)

	firstOpt := WithMaxWALSize(firstSize)
	firstOpt(db)

	if db.maxWALSize != firstSize {
		t.Errorf("Expected maxWALSize to be %d, got %d", firstSize, db.maxWALSize)
	}

	secondOpt := WithMaxWALSize(secondSize)
	secondOpt(db)

	if db.maxWALSize != secondSize {
		t.Errorf("Expected maxWALSize to be %d, got %d", secondSize, db.maxWALSize)
	}
}

func TestWithMaxWALSizeZeroValueDoesNotChangeMaxWALSize(t *testing.T) {
	db := &TiempoDB{}
	initialSize := db.maxWALSize

	opt := WithMaxWALSize(0)
	opt(db)

	if db.maxWALSize != initialSize {
		t.Errorf("Expected maxWALSize to remain %d, got %d", initialSize, db.maxWALSize)
	}
}

func TestWithMaxWALSizeNegativeValueDoesNotChangeMaxWALSize(t *testing.T) {
	db := &TiempoDB{}
	initialSize := db.maxWALSize

	opt := WithMaxWALSize(-1)
	opt(db)

	if db.maxWALSize != initialSize {
		t.Errorf("Expected maxWALSize to remain %d, got %d", initialSize, db.maxWALSize)
	}
}
