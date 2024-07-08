// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package bytes

import (
	"fmt"
	"reflect"
)

type FastPacket interface {
	Pack(w *BytesBuffer) error
	UnPack(w *BytesBuffer, o interface{}) error
}

func Pack(w *BytesBuffer, o interface{}) ([]byte, error) {
	if o == nil {
		return w.Data(), nil
	}

	if fast, ok := o.(FastPacket); ok {
		if err := fast.Pack(w); err != nil {
			return nil, err
		}

		return w.Data(), nil
	}

	if err := pack(reflect.ValueOf(o), w); err != nil {
		return nil, err
	}
	return w.Data(), nil
}

func UnPack(w *BytesBuffer, o interface{}) error {
	if o == nil {
		return nil
	}

	if fast, ok := o.(FastPacket); ok {
		return fast.UnPack(w, o)
	}

	return unpack(reflect.ValueOf(o), w)
}

func pack(v reflect.Value, w *BytesBuffer) (err error) {
	switch v.Kind() {
	case reflect.Bool:
		err = w.WriteBool(v.Bool())
	case reflect.Uint8:
		err = w.WriteUint8(uint8(v.Uint()))
	case reflect.Uint16:
		err = w.WriteUint16(uint16(v.Uint()))
	case reflect.Uint32:
		err = w.WriteUint32(uint32(v.Uint()))
	case reflect.Uint64:
		err = w.WriteUint64(uint64(v.Uint()))
	case reflect.Int8:
		err = w.WriteInt8(int8(v.Int()))
	case reflect.Int16:
		err = w.WriteInt16(int16(v.Int()))
	case reflect.Int32:
		err = w.WriteInt32(int32(v.Int()))
	case reflect.Int64:
		err = w.WriteInt64(int64(v.Int()))
	case reflect.Float32:
		err = w.WriteFloat32(float32(v.Float()))
	case reflect.Float64:
		err = w.WriteFloat64(v.Float())
	case reflect.String:
		err = w.WriteString(v.String())
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return nil
		}

		err = pack(v.Elem(), w)

	case reflect.Slice:
		if bs, ok := v.Interface().([]byte); ok {
			err = w.WriteUint16Bytes(bs)
		} else {
			size := v.Len()
			if err := w.WriteUint16(uint16(size)); err != nil {
				return err
			}

			for i := 0; i < size; i++ {
				if err := pack(v.Index(i), w); err != nil {
					return err
				}
			}
		}

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := pack(v.Field(i), w); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("Unexpected type: %v", v.Kind())
	}

	return
}

func unpack(v reflect.Value, w *BytesBuffer) error {
	switch v.Kind() {
	case reflect.Bool:
		if b, err := w.ReadBool(); err != nil {
			return err
		} else {
			v.SetBool(b)
		}

	case reflect.Uint8:
		if b, err := w.ReadUint8(); err != nil {
			return err
		} else {
			v.SetUint(uint64(b))
		}

	case reflect.Uint16:
		if b, err := w.ReadUint16(); err != nil {
			return err
		} else {
			v.SetUint(uint64(b))
		}

	case reflect.Uint32:
		if b, err := w.ReadUint32(); err != nil {
			return err
		} else {
			v.SetUint(uint64(b))
		}

	case reflect.Uint64:
		if b, err := w.ReadUint64(); err != nil {
			return err
		} else {
			v.SetUint(b)
		}

	case reflect.Int8:
		if b, err := w.ReadInt8(); err != nil {
			return err
		} else {
			v.SetInt(int64(b))
		}

	case reflect.Int16:
		if b, err := w.ReadInt16(); err != nil {
			return err
		} else {
			v.SetInt(int64(b))
		}

	case reflect.Int32:
		if b, err := w.ReadInt32(); err != nil {
			return err
		} else {
			v.SetInt(int64(b))
		}

	case reflect.Int64:
		if b, err := w.ReadInt64(); err != nil {
			return err
		} else {
			v.SetInt(b)
		}

	case reflect.Float32:
		if b, err := w.ReadFloat32(); err != nil {
			return err
		} else {
			v.SetFloat(float64(b))
		}

	case reflect.Float64:
		if b, err := w.ReadFloat64(); err != nil {
			return err
		} else {
			v.SetFloat(b)
		}

	case reflect.String:
		if b, err := w.ReadString(); err != nil {
			return err
		} else {
			v.SetString(b)
		}

	case reflect.Ptr, reflect.Interface:
		if err := unpack(v.Elem(), w); err != nil {
			return err
		}

	case reflect.Slice:
		if _, ok := v.Interface().([]byte); ok {
			if b, err := w.ReadUint16Bytes(); err != nil {
				return err
			} else {
				v.SetBytes(b)
			}

		} else {
			size, err := w.ReadUint16()
			if err != nil {
				return err
			}

			iSize := int(size)
			data := reflect.MakeSlice(v.Type(), iSize, iSize*2)
			for i := 0; i < iSize; i++ {
				if err := unpack(data.Index(i), w); err != nil {
					return err
				}
			}
			v.Set(data)
		}

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := unpack(v.Field(i), w); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("Unexpected type: %v", v.Kind())
	}

	return nil
}
