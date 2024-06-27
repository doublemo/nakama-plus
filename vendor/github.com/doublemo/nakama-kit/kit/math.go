// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
	mrand "math/rand"
	"sync/atomic"
	"time"
)

var (
	random atomic.Pointer[mrand.Rand]
)

func Rand() *mrand.Rand {
	if r := random.Load(); r != nil {
		return r
	}

	r := mrand.New(mrand.NewSource(int64(time.Now().Nanosecond())))
	if !random.CompareAndSwap(nil, r) {
		return random.Load()
	}
	return r
}

func RandomWheelWithBigInt(rate, base *big.Int) (bool, error) {
	cmp := rate.Cmp(base)
	if cmp >= 0 {
		return true, nil
	}

	rnd, err := rand.Int(rand.Reader, base)
	if err != nil {
		return false, err
	}

	m := make(map[uint64]bool)
	one := big.NewInt(1)
	i := big.NewInt(0)
	counter := big.NewInt(0)
	maxLoop := big.NewInt(100)
	for i.Cmp(rate) < 0 && counter.Cmp(maxLoop) < 0 {
		n, err := rand.Int(rand.Reader, base)
		if err != nil {
			return false, err
		}
		if m[n.Uint64()] {
			counter.Add(counter, one)
			continue
		}
		m[n.Uint64()] = true
		i.Add(i, one)
	}
	return m[rnd.Uint64()], nil
}

func RandomWheel(rate, base float64) bool {
	if rate >= base {
		return true
	}
	r := Rand()
	rnd := rate / base
	return rnd > r.Float64()
}

func RandomBetweenFloat64(min, max float64) float64 {
	if max <= min {
		return min
	}
	return min + Rand().Float64()*(max-min)
}

func RandomBetween(min, max int) int {
	return Rand().Intn(max-min+1) + min
}

func RandomBetweenV2(min int, max int) int {
	if max <= min {
		return min
	}
	result, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return int(result.Int64()) + min
}

func RandomHex(n int) (string, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

var randomStringPool = "abcdefghijklmnopqrstuvwxyzABCEFGHIJKLMNOPQRSTUVWXYZ:|?$%@][{}#&/()*"

func RandomString(n int) string {
	r := Rand()
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = randomStringPool[r.Intn(len(randomStringPool))]
	}

	return string(bytes)
}
