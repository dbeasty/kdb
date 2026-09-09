//go:build !unix

package io

import "errors"

// availableBytes has no portable implementation off unix. An error is the honest answer: a caller
// deciding whether something will fit must be able to tell "no room" from "cannot tell".
func availableBytes(string) (int64, error) {
	return 0, errors.New("free space is not available on this platform")
}
