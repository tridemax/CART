//---------------------------------------------------------------------
// Copyright (c) 2012 Maksym Diachenko.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//---------------------------------------------------------------------

#pragma once

namespace CART
{
	//---------------------------------------------------------------------
	/// Murmur3 Hash implementation with 64-bit key.
	//---------------------------------------------------------------------
	class HashMurmur3
	{
	public:
		typedef unsigned __int64 hash_t;

	public:
		//---------------------------------------------------------------------
		/// Constructor.
		inline HashMurmur3()
		{
			m_value = 1;
		}

		//---------------------------------------------------------------------
		/// Gets HashMurmur3 value directly.
		hash_t GetInternalValue() const
		{
			return m_value;
		}

		//---------------------------------------------------------------------
		/// Gets HashMurmur3 value directly.
		operator hash_t() const
		{
			return m_value;
		}

		//---------------------------------------------------------------------
		/// Sets HashMurmur3 value directly.
		void SetInternalValue(hash_t value)
		{
			m_value = value;
		}

		//---------------------------------------------------------------------
		/// Constructor.
		inline HashMurmur3(const HashMurmur3& op)
		{
			m_value = op.m_value;
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		inline HashMurmur3& Set(const HashMurmur3& op)
		{
			m_value = op.m_value;
			return *this;
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		inline HashMurmur3& operator =(const HashMurmur3& op)
		{
			m_value = op.m_value;
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		inline HashMurmur3& Add(const HashMurmur3& op)
		{
			m_value += op.m_value; // this is not actually correct
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		inline HashMurmur3& operator +=(const HashMurmur3& op)
		{
			m_value += op.m_value; // this is not actually correct
			return *this;
		}

		//---------------------------------------------------------------------
		/// Constructor.
		inline HashMurmur3(const void* buf, hash_t bufSize)
		{
			m_value = Calculate(buf, bufSize);
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		inline HashMurmur3& Set(const void* buf, hash_t bufSize)
		{
			m_value = Calculate(buf, bufSize);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		inline HashMurmur3& Add(const void* buf, hash_t bufSize)
		{
			m_value = Calculate(buf, bufSize, m_value);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Constructor.
		inline HashMurmur3(const char* string)
		{
			m_value = Calculate(string, strlen(string));
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		inline HashMurmur3& Set(const char* string)
		{
			m_value = Calculate(string, strlen(string));
			return *this;
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		inline HashMurmur3& operator =(const char* string)
		{
			m_value = Calculate(string, strlen(string));
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		inline HashMurmur3& Add(const char* string)
		{
			m_value = Calculate(string, strlen(string), m_value);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		inline HashMurmur3& operator +=(const char* string)
		{
			m_value = Calculate(string, strlen(string), m_value);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Constructor.
		inline HashMurmur3(const wchar_t* string)
		{
			m_value = Calculate(string, wcslen(string) * sizeof(wchar_t));
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		inline HashMurmur3& Set(const wchar_t* string)
		{
			m_value = Calculate(string, wcslen(string) * sizeof(wchar_t));
			return *this;
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		inline HashMurmur3& operator =(const wchar_t* string)
		{
			m_value = Calculate(string, wcslen(string) * sizeof(wchar_t));
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		inline HashMurmur3& Add(const wchar_t* string)
		{
			m_value = Calculate(string, wcslen(string) * sizeof(wchar_t), m_value);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		inline HashMurmur3& operator +=(const wchar_t* string)
		{
			m_value = Calculate(string, wcslen(string) * sizeof(wchar_t), m_value);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Constructor.
		template <typename T>
		inline HashMurmur3(const T& value)
		{
			m_value = Calculate(&value, sizeof(T));
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		template <typename T>
		inline HashMurmur3& Set(const T& value)
		{
			m_value = Calculate(&value, sizeof(T));
			return *this;
		}

		//---------------------------------------------------------------------
		/// Sets this value.
		template <typename T>
		inline HashMurmur3& operator =(const T& value)
		{
			m_value = Calculate(&value, sizeof(T));
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		template <typename T>
		inline HashMurmur3& Add(const T& value)
		{
			m_value = Calculate(&value, sizeof(T), m_value);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Adds to this value.
		template <typename T>
		inline HashMurmur3& operator +=(const T& value)
		{
			m_value = Calculate(&value, sizeof(T), m_value);
			return *this;
		}

		//---------------------------------------------------------------------
		/// Compares values.
		bool operator ==(const hash_t value) const
		{
			return m_value == value;
		}

		//---------------------------------------------------------------------
		/// Compares values.
		bool operator ==(const HashMurmur3& op) const
		{
			return m_value == op.m_value;
		}

		//---------------------------------------------------------------------
		/// Compares values.
		bool operator !=(const hash_t value) const
		{
			return m_value != value;
		}

		//---------------------------------------------------------------------
		/// Compares values.
		bool operator !=(const HashMurmur3& op) const
		{
			return m_value != op.m_value;
		}

	private:
		//---------------------------------------------------------------------
		inline static hash_t rotl64(hash_t x, signed char r)
		{
			return (x << r) | (x >> (64 - r));
		}

		//---------------------------------------------------------------------
		inline static hash_t fmix(hash_t k)
		{
			k ^= k >> 33;
			k *= 0xff51afd7ed558ccd;
			k ^= k >> 33;
			k *= 0xc4ceb9fe1a85ec53;
			k ^= k >> 33;

			return k;
		}

		//---------------------------------------------------------------------
		/// Calculates HashMurmur3 for the buffer.
		inline static hash_t Calculate(const void* buf, hash_t bufLen, hash_t startValue = 0)
		{
			const unsigned char* data = (const unsigned char*)buf;
			const hash_t nblocks = bufLen / 16;

			hash_t h1 = startValue >> 32;
			hash_t h2 = startValue & 0xFFFFFFFF;

			hash_t c1 = 0x87c37b91114253d5;
			hash_t c2 = 0x4cf5ad432745937f;

			// body
			const hash_t* blocks = (const uint64_t *)(data);
			for (int i = 0; i < nblocks; ++i)
			{
				hash_t k1 = blocks[i * 2 + 0];
				hash_t k2 = blocks[i * 2 + 1];

				k1 *= c1; k1 = rotl64(k1, 31); k1 *= c2; h1 ^= k1;
				h1 = rotl64(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;
				k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2;
				h2 = rotl64(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
			}

			// tail
			const unsigned char* tail = (const unsigned char*)(data + nblocks * 16);

			hash_t k1 = 0;
			hash_t k2 = 0;

			switch (bufLen & 15)
			{
			case 15: k2 ^= hash_t(tail[14]) << 48;
			case 14: k2 ^= hash_t(tail[13]) << 40;
			case 13: k2 ^= hash_t(tail[12]) << 32;
			case 12: k2 ^= hash_t(tail[11]) << 24;
			case 11: k2 ^= hash_t(tail[10]) << 16;
			case 10: k2 ^= hash_t(tail[ 9]) << 8;
			case  9: k2 ^= hash_t(tail[ 8]) << 0;
			k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2;

			case 8: k1 ^= hash_t(tail[7]) << 56;
			case 7: k1 ^= hash_t(tail[6]) << 48;
			case 6: k1 ^= hash_t(tail[5]) << 40;
			case 5: k1 ^= hash_t(tail[4]) << 32;
			case 4: k1 ^= hash_t(tail[3]) << 24;
			case 3: k1 ^= hash_t(tail[2]) << 16;
			case 2: k1 ^= hash_t(tail[1]) << 8;
			case 1: k1 ^= hash_t(tail[0]) << 0;
			k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;
			};

			// finalization
			h1 ^= bufLen; h2 ^= bufLen;

			h1 += h2;
			h2 += h1;

			h1 = fmix(h1);
			h2 = fmix(h2);

			h1 += h2;
			h2 += h1;

			return h1; // h2 contains bunch of equally mixed bits - can be used to output 128 bit value
		}

	private:
		hash_t						m_value;
	};
};