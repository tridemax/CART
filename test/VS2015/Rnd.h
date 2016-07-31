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

#include "time.h"

//-------------------------------------------------------------------
/// Random numbers generator.
//-------------------------------------------------------------------
class Rnd
{
	// Period parameters
	static const int N = 624;
	static const int M = 397;
	static const unsigned int MATRIX_A = 0x9908b0df;
	static const unsigned int UPPER_MASK = 0x80000000;
	static const unsigned int LOWER_MASK = 0x7fffffff;

	// Tempering parameters
	static const unsigned int TEMPERING_MASK_B = 0x9d2c5680;
	static const unsigned int TEMPERING_MASK_C = 0xefc60000;

	inline unsigned int TEMPERING_SHIFT_U(unsigned int y)	{ return (y >> 11); }
	inline unsigned int TEMPERING_SHIFT_S(unsigned int y)	{ return (y << 7); }
	inline unsigned int TEMPERING_SHIFT_T(unsigned int y)	{ return (y << 15); }
	inline unsigned int TEMPERING_SHIFT_L(unsigned int y)	{ return (y >> 18); }

public:
	//-------------------------------------------------------------------
	/// @brief				Constructor with starting seed for generator.
	/// @param baseSeed		Starting seed.
	/// @param cacheSize	Pre-generated numbers cache size, must be multiple of 4.
	inline Rnd(unsigned int baseSeed = (unsigned int)time(0))
	{
		SetSeed(baseSeed);
	}

	//-------------------------------------------------------------------
	/// Destructor.
	inline ~Rnd()
	{
	}

	//-------------------------------------------------------------------
	/// Returns current seed. Set it back to randomizer and this will guarantee reproducible sequence of random numbers.
	inline unsigned int GetSeed()
	{
		m_seed = m_aMT[m_iMTI];
		SetStartVector();
		return m_seed;
	}

	//-------------------------------------------------------------------
	/// Sets current seed to reproduce sequence saved earlier with GetSeed().
	inline void SetSeed(unsigned int newSeed)
	{
		if (newSeed == 0)
			newSeed = 1;

		m_seed = newSeed;
		SetStartVector();
	}

	//-------------------------------------------------------------------
	/// Generates random u32. Fast.
	inline unsigned int RandomU32()
	{
		return Draw();
	}

	//-------------------------------------------------------------------
	/// Generates random float in range [0...1]. Fast.
	inline float RandomF32()
	{
		double draw = (double)Draw();
		return float(draw / 0xFFFFFFFF);
	}

	//---------------------------------------------------------------------
	/// Generates random float in range [0...1). Fast.
	inline float RandomF32Exc()
	{
		double draw = (double)Draw();
		return float(draw / (0xFFFFFFFF - 1));
	}

	//---------------------------------------------------------------------
	/// Generates random number in range [min, max).
	template <typename T>
	inline T RandomRange(T min, T max)
	{
		if (min >= max)
			return min;

		double draw = ((double)Draw()) / (0xFFFFFFFF - 1);
		double step = (double)T(double(max) - double(min) + 1.0 / double(0xFFFFFFFF - 1));
		return min + T(step * draw);
	}


private:
	//---------------------------------------------------------------------
	/// Sets starting vector and clears cache.
	inline void SetStartVector()
	{
		m_aMT[0] = m_seed & 0xffffffff;

		for (int mti = 1; mti < N; mti++)
			m_aMT[mti] = (69069 * m_aMT[mti-1]) & 0xFFFFFFFF;

		m_iMTI = N;
	}

	//---------------------------------------------------------------------
	/// Returns random 32-bit number.
	inline unsigned int Draw()
	{
		static unsigned int mag01[2] = { 0x0, MATRIX_A };
		unsigned int y;

		if (m_iMTI >= N)
		{
			// Generate N words
			int kk;

			for (kk = 0; kk < N - M; kk++)
			{
				y = (m_aMT[kk] & UPPER_MASK) | (m_aMT[kk+1] & LOWER_MASK);
				m_aMT[kk] = m_aMT[kk+M] ^ (y >> 1) ^ mag01[y & 0x1];
			}

			for (; kk < N-1; kk++)
			{
				y = (m_aMT[kk] & UPPER_MASK) | (m_aMT[kk+1] & LOWER_MASK);
				m_aMT[kk] = m_aMT[kk+(M-N)] ^ (y >> 1) ^ mag01[y & 0x1];
			}

			y = (m_aMT[N-1] & UPPER_MASK) | (m_aMT[0] & LOWER_MASK);
			m_aMT[N-1] = m_aMT[M-1] ^ (y >> 1) ^ mag01[y & 0x1];

			m_iMTI = 0;
		}

		y = m_aMT[m_iMTI++];
		y ^= TEMPERING_SHIFT_U(y);
		y ^= TEMPERING_SHIFT_S(y) & TEMPERING_MASK_B;
		y ^= TEMPERING_SHIFT_T(y) & TEMPERING_MASK_C;
		y ^= TEMPERING_SHIFT_L(y);

		return y;
	}


private:
	unsigned int			m_seed;		///< base seed
	unsigned int			m_aMT[624];	///< random numbers cache
	int						m_iMTI;		///< current random number
};