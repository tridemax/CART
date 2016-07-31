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

#define TBB_PREVIEW_CONCURRENT_LRU_CACHE 1
#include "TBB/tbb.h"
#include "TBB/concurrent_lru_cache.h"
#include "Rnd.h"
#include "../../Include/Cache.h"

#include <iostream>
#include <conio.h>

typedef unsigned int key_t;

//---------------------------------------------------------------------
// Implementation of cache value acquiring and releasing.
struct CacheImpl
{
	key_t* CacheAcquireValue(key_t key) { ++cacheMisses; return new key_t(key); }
	void CacheReleaseValue(key_t key, key_t* value) { return delete value; }
	size_t CacheGetValueSize(key_t* value) { return sizeof(key_t); }

	CacheImpl() { tries = 0; cacheMisses = 0; }

	tbb::atomic<int> tries;
	tbb::atomic<int> cacheMisses;
};

//---------------------------------------------------------------------
// LRU cache values acquiring and releasing.
tbb::atomic<int> LRUTries;
tbb::atomic<int> LRUCacheMisses;

struct LRUImpl
{
	key_t operator()(key_t key) { ++LRUCacheMisses; return key; }
	LRUImpl() { LRUTries = 0; LRUCacheMisses = 0; }
};

//---------------------------------------------------------------------
// Random bins ranges.
struct Range { int start; int len; };
Range ranges[6] = { { 0, 150 }, { 150, 350 }, { 500, 500 }, { 1000, 1500 }, { 2500, 2500 }, { 5000, 10000 } };
const unsigned int entire_range = 10000;

//---------------------------------------------------------------------
int main(int argc, char* argv[])
{
	const int total_num_tries = 1005000;
	const int cache_sizes[3] = { 100, 500, 1000 };

	std::cout << "Less is better.\n";

	for (int i = 0; i < 3; ++i)
	{
		CacheImpl CARTCacheImpl;
		CART::Cache<key_t, key_t, CacheImpl> CARTCache(CARTCacheImpl, cache_sizes[i], 0);

		LRUImpl LRUCacheImpl;
		tbb::concurrent_lru_cache<key_t, key_t, LRUImpl> LRUCache(LRUCacheImpl, cache_sizes[i]);

		Rnd rnd(1);

		for (int j = 0; j < total_num_tries; ++j)
		{
			key_t val = rnd.RandomRange(0u, entire_range);

			auto handleCAR = CARTCache[val];
			++CARTCacheImpl.tries;

			auto handleLRU = LRUCache[val];
			++LRUTries;
		}

		std::cout << "Random numbers, cache size " << cache_sizes[i] << "\n";

		std::cout << "  CART result: " << (double)(CARTCacheImpl.cacheMisses) / (double)(CARTCacheImpl.tries) <<
			", missed " << CARTCacheImpl.cacheMisses << " / " << CARTCacheImpl.tries << "\n";
		std::cout << "  LRU result: " << (double)(LRUCacheMisses) / (double)(LRUTries) <<
			", missed " << LRUCacheMisses << " / " << LRUTries << "\n";
	}

	for (int i = 0; i < 3; ++i)
	{
		CacheImpl CARTCacheImpl;
		CART::Cache<key_t, key_t, CacheImpl> CARTCache(CARTCacheImpl, cache_sizes[i], 0);

		LRUImpl LRUCacheImpl;
		tbb::concurrent_lru_cache<key_t, key_t, LRUImpl> LRUCache(LRUCacheImpl, cache_sizes[i]);

		Rnd rnd(1);

		for (int j = 0; j < total_num_tries; ++j)
		{
			unsigned int rangeIdx = rnd.RandomRange(0u, 6u);
			key_t val = rnd.RandomRange(ranges[rangeIdx].start, ranges[rangeIdx].start + ranges[rangeIdx].len);

			auto handleCAR = CARTCache[val];
			++CARTCacheImpl.tries;

			auto handleLRU = LRUCache[val];
			++LRUTries;
		}

		std::cout << "Bins draw, cache size " << cache_sizes[i] << "\n";

		std::cout << "  CART result: " << (double)(CARTCacheImpl.cacheMisses) / (double)(CARTCacheImpl.tries) <<
			", missed " << CARTCacheImpl.cacheMisses << " / " << CARTCacheImpl.tries << "\n";
		std::cout << "  LRU result: " << (double)(LRUCacheMisses) / (double)(LRUTries) <<
			", missed " << LRUCacheMisses << " / " << LRUTries << "\n";
	}

	std::cout << "\nPress any key to quit...\n";
	_getch();

	return 0;
}

