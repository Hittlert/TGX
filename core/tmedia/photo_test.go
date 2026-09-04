package tmedia

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestGetPhotoSize_ResolutionPriority(t *testing.T) {
	// Case matching P-SM-0037:
	// Size 1: 800x451, standard JPEG, size 87837 (lower resolution, but less compressed -> larger bytes)
	// Size 2: 873x492, progressive JPEG, size 80333 (higher resolution, better compression -> smaller bytes)
	sizes := []tg.PhotoSizeClass{
		&tg.PhotoSize{
			Type: "m",
			W:    800,
			H:    451,
			Size: 87837,
		},
		&tg.PhotoSizeProgressive{
			Type:  "x",
			W:     873,
			H:     492,
			Sizes: []int{20000, 50000, 80333},
		},
	}

	tp, sz, ok := GetPhotoSize(sizes)
	if !ok {
		t.Fatalf("expected photo size found, got not found")
	}
	if tp != "x" || sz != 80333 {
		t.Fatalf("expected type 'x' size 80333, got type '%s' size %d", tp, sz)
	}
}

func TestGetPhotoSize_SameResolutionLargerSize(t *testing.T) {
	sizes := []tg.PhotoSizeClass{
		&tg.PhotoSize{
			Type: "a",
			W:    800,
			H:    600,
			Size: 50000,
		},
		&tg.PhotoSize{
			Type: "b",
			W:    800,
			H:    600,
			Size: 60000,
		},
	}

	tp, sz, ok := GetPhotoSize(sizes)
	if !ok {
		t.Fatalf("expected photo size found, got not found")
	}
	if tp != "b" || sz != 60000 {
		t.Fatalf("expected type 'b' size 60000, got type '%s' size %d", tp, sz)
	}
}
