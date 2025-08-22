// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	mrand "math/rand/v2" // 使用新的rand/v2包
)

func RandomInt32(values []int32, total uint64) int {
	if len(values) == 0 {
		return -1
	}

	if total == 0 {
		return -1
	}

	rnd := Uint64N(total)
	var sum uint64

	for i, weight := range values {
		sum += uint64(weight)
		if rnd < sum {
			return i
		}
	}

	return 0
}

func Uint64N(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	r := binary.BigEndian.Uint64(buf[:])
	return r % n
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
	rnd := rate / base
	return rnd > mrand.Float64()
}

func RandomBetweenFloat64(min, max float64) float64 {
	if max <= min {
		return min
	}
	return min + mrand.Float64()*(max-min)
}

func RandomBetween(min, max int) int {
	if max < min {
		max = min // 确保 max >= min
	}

	// 计算范围（包含 max，所以是 max - min + 1）
	rangeSize := max - min + 1

	// 1. 优先使用 crypto/rand（密码学安全随机数）
	randNum, err := rand.Int(rand.Reader, big.NewInt(int64(rangeSize)))
	if err == nil {
		return min + int(randNum.Int64())
	}

	// 2. 如果 crypto/rand 失败，回退到 math/rand/v2（高性能随机数）
	return min + mrand.IntN(rangeSize)
}

func BetweenN(min int, max int) int {
	if max <= min {
		return min
	}

	// 计算范围，避免重复计算
	rangeSize := max - min

	// 使用 crypto/rand 生成更安全的随机数
	randNum, err := rand.Int(rand.Reader, big.NewInt(int64(rangeSize)))
	if err == nil {
		return min + int(randNum.Int64())
	}

	// 如果 crypto/rand 失败，回退到 math/rand/v2（更快且比 v1 更均匀）
	return min + mrand.IntN(rangeSize)
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
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = randomStringPool[mrand.IntN(len(randomStringPool))]
	}

	return string(bytes)
}
