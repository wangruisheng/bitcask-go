package index

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// 测试是不是生成从0-1的随机数
func Test_random(t *testing.T) {
	tests := []struct {
		name string
		want float64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, random(), "random()")
		})
	}
}
