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

#include <assert.h>
#include <deque>
#include <list>
#include <unordered_map>
#include "HashMurmur3.h"

namespace CART
{
	//---------------------------------------------------------------------
	template <typename T, typename F> inline T Max(T x, F y)
	{
		return (x < T(y)) ? T(y) : x;
	}
	//---------------------------------------------------------------------
	template <typename T, typename F> inline T Min(T x, F y)
	{
		return (x < T(y)) ? x : T(y);
	}
	//---------------------------------------------------------------------
	/// Compound key / user data container, that might be used as KEY in Cache container.
	//---------------------------------------------------------------------
	template <typename KEY, typename USERDATA>
	struct CacheCompoundKey
	{
		KEY			key;
		USERDATA	userData;

		//---------------------------------------------------------------------
		inline explicit CacheCompoundKey() { }
		//---------------------------------------------------------------------
		inline CacheCompoundKey(KEY key, USERDATA&& userData) : key(key), userData(std::move(userData)) { }
		//---------------------------------------------------------------------
		operator size_t() const { return (size_t)key; }
		//---------------------------------------------------------------------
		bool operator==(const CacheCompoundKey& op) const { return key == op.key; }
	};


	//---------------------------------------------------------------------
	template <typename KEY, typename VALUE>
	struct ICacheControl
	{
		//---------------------------------------------------------------------
		/// Loads value by key from persistent storage.
		VALUE* CacheAcquireValue(KEY key);
		//---------------------------------------------------------------------
		/// Releases value, it can be safely deleted.
		void CacheReleaseValue(KEY key, VALUE* value);
		//---------------------------------------------------------------------
		/// Returns size of the value in bytes.
		size_t CacheGetValueSize(VALUE* value);
	};


	//---------------------------------------------------------------------
	/// Multi-threaded cache implementation, inspired by Clock with Adaptive Replacement and Temporal Filtering algorithm.
	/// Somewhat better than classic LRU caching strategy, with comparable execution speed.
	/// http://usenix.org/legacy/publications/library/proceedings/fast04/tech/full_papers/bansal/bansal.pdf
	///
	/// If you need to include user data into key - use CacheCompoundKey implementation as KEY template argument.
	/// INTERFACE class should implement ICacheControl interface.
	/// OBJ_SIZE_CAN_CHANGE control how often cache will query for object size.
	//---------------------------------------------------------------------
	template <typename KEY, typename VALUE, typename INTERFACE>
	class Cache
	{
	public:
		//---------------------------------------------------------------------
		class Handle
		{
			Cache*					m_cache = nullptr;
			KEY						m_key;
			VALUE*					m_value = nullptr;

		public:
			//---------------------------------------------------------------------
			Handle() { }

			//---------------------------------------------------------------------
			Handle(Cache<KEY, VALUE, INTERFACE>* cache, KEY key, VALUE* value) : m_cache(cache), m_key(key), m_value(value)
			{
				if (m_cache && m_value)
					m_cache->IncUsage(m_key, m_value);
			}

			//---------------------------------------------------------------------
			Handle(const Handle& op) : m_cache(op.m_cache), m_key(op.m_key), m_value(op.m_value)
			{
				if (m_cache && m_value)
					m_cache->IncUsage(m_key, m_value);
			}

			//---------------------------------------------------------------------
			Handle& operator=(const Handle& op)
			{
				Release();
				m_cache = op.m_cache; m_key = op.m_key; m_value = op.m_value;

				if (m_cache && m_value)
					m_cache->IncUsage(m_key, m_value);

				return *this;
			}

			//---------------------------------------------------------------------
			Handle(Handle&& op) : m_cache(op.m_cache), m_key(op.m_key), m_value(op.m_value)
			{
				op.m_cache = nullptr; op.m_value = nullptr;
			}

			//---------------------------------------------------------------------
			Handle& operator=(Handle&& op)
			{
				Release();
				m_cache = op.m_cache; m_key = op.m_key; m_value = op.m_value;
				op.m_cache = nullptr; op.m_value = nullptr;
				return *this;
			}

			//---------------------------------------------------------------------
			~Handle() { Release(); }

			//---------------------------------------------------------------------
			void Release()
			{
				if (m_cache && m_value)
				{
					bool isZero = m_cache->DecUsage(m_key, m_value, false);
					assert(!isZero); // handle should not be the last one holding the lock
					m_value = nullptr;
				}
			}

			//---------------------------------------------------------------------
			VALUE& value() { return *m_value; }
			const VALUE& value() const { return *m_value; }
			const KEY key() const { return m_key; }
			VALUE& operator *() { return *m_value; }
			const VALUE& operator *() const { return *m_value; }
			VALUE& GetValue() { return *m_value; }
			bool IsEmpty() const { return m_value == nullptr; }
			Cache* GetCache() const { return m_cache; }
			Handle Duplicate() const { return Handle(*this); }
		};

	public:
		//---------------------------------------------------------------------
		/// @brief				Constructor.
		/// @param functor		Functor to acquire and release values from persistent storage.
		/// @param maxNumElements Maximum number of elements this instance supports. Can be zero for unlimited number of elements.
		/// @param maxUsedMemory Maximum amount of memory this instance will consume. Can be zero for unlimited amount of memory.
		inline Cache(INTERFACE& _interface, size_t maxNumElements, size_t maxUsedMemory) : m_interface(_interface)
		{
			assert(maxNumElements || maxUsedMemory);

			m_maxNumElements = maxNumElements;
			m_maxUsedMemory = maxUsedMemory;

			Clear();
		}

		//---------------------------------------------------------------------
		/// Destructor.
		inline ~Cache()
		{
			Clear();
		}

		//---------------------------------------------------------------------
		inline INTERFACE& GetInterface()
		{
			return m_interface;
		}

		//---------------------------------------------------------------------
		/// Finds value by key. Creates value if it does not exists.
		inline Handle FindOrCreate(KEY key)
		{
			typename tbb::concurrent_hash_map<KEY, CLOCKElement*>::const_accessor cacheAccessor;
			if (m_T1T2map.find(cacheAccessor, key))
			{
				cacheAccessor->second->referenceBit = true; // set reference bit
				return Handle(this, key, cacheAccessor->second->page);
			}

			// Cache miss
			cacheAccessor.release();

			return InternalInsert(key, nullptr);
		}

		//---------------------------------------------------------------------
		/// Finds value by key. Creates value if it does not exists.
		inline Handle operator[](KEY key)
		{
			return FindOrCreate(key);
		}

		//---------------------------------------------------------------------
		/// Checks if value exists in cache. Returns empty handle, if value does not exists.
		/// This function does not alter cache state. It is only checks if value exists, in difference from Find().
		inline Handle IsInCache(KEY key)
		{
			typename tbb::concurrent_hash_map<KEY, CLOCKElement*>::const_accessor cacheAccessor;
			if (m_T1T2map.find(cacheAccessor, key))
				return Handle(this, key, cacheAccessor->second->page);

			return Handle();
		}

		//---------------------------------------------------------------------
		/// Inserts value into cache. Does not updates existing values.
		/// Check returned value(). If it is different from value pointer you submitted - do not forget to delete your submitted value because another thread inserted it already.
		inline Handle InsertIntoCache(KEY key, VALUE* value)
		{
			return InternalInsert(key, value);
		}

		//---------------------------------------------------------------------
		/// Removes value from cache. Use only if you are 100% sure this value will no longer be used again.
		inline void RemoveFromCache(KEY key)
		{
			return InternalRemove(key);
		}

		//---------------------------------------------------------------------
		/// Clears cache.
		inline void Clear()
		{
			for (auto it = m_T1Queue.begin(); it != m_T1Queue.end(); ++it)
			{
				DecUsage((*it)->pageKey, (*it)->page, true);
				delete *it;
			}
			m_T1Queue.clear();

			for (auto it = m_T2Queue.begin(); it != m_T2Queue.end(); ++it)
			{
				DecUsage((*it)->pageKey, (*it)->page, true);
				delete *it;
			}
			m_T2Queue.clear();

			for (auto it = m_B1List.begin(); it != m_B1List.end(); ++it)
				delete *it;
			m_B1List.clear();
			m_B1ListSize = 0;

			for (auto it = m_B2List.begin(); it != m_B2List.end(); ++it)
				delete *it;
			m_B2List.clear();
			m_B2ListSize = 0;

			m_p = 0;
			m_q = 0;
			m_numShort = 0;
			m_numLong = 0;
			m_usedMemory = 0;

			m_T1T2map.clear();
			m_B1B2map.clear();
		}

	protected:
		//---------------------------------------------------------------------
		inline bool IsFull() const
		{
			return (m_maxNumElements && (m_T1Queue.size() + m_T2Queue.size() >= m_maxNumElements)) || (m_maxUsedMemory && m_usedMemory >= m_maxUsedMemory);
		}

		//---------------------------------------------------------------------
		inline Handle InternalInsert(KEY key, VALUE* value)
		{
			// Lock current key from reading/modification, effectively locking subsequent reads
			typename tbb::concurrent_hash_map<KEY, CLOCKElement*>::accessor writeAccessor;
			if (!m_T1T2map.insert(writeAccessor, key))
				return Handle(this, key, writeAccessor->second->page); // another thread inserted it meanwhile

			// Fetch a page, if needed
			if (value == nullptr)
				value = m_interface.CacheAcquireValue(key);

			IncUsage(key, value);

			// Protect structure with lock
			tbb::queuing_mutex::scoped_lock lock(m_mutex);

			unsigned __int64 maxNumElements = m_maxNumElements;

			// Insert page into cache
			if (IsFull())
			{
				maxNumElements = m_T1Queue.size() + m_T2Queue.size();

				// Cache is full - replace a page from cache
				while (!m_T2Queue.empty() && m_T2Queue.front()->referenceBit)
				{
					// Move head page in T2 to tail position in T1. Set the page reference bit to 0.
					m_T1Queue.push_back(m_T2Queue.front());
					m_T2Queue.pop_front();
					
					CLOCKElement* element = m_T1Queue.back();
					element->referenceBit = false;
					element->firstList = true;

					if ((int)m_T2Queue.size() + m_B2ListSize + (int)m_T1Queue.size() - m_numShort >= maxNumElements)
						m_q = (int)Min(m_q + 1, 2 * maxNumElements - m_T1Queue.size());
				}

				while (m_T1Queue.size() && (m_T1Queue.front()->longBit || m_T1Queue.front()->referenceBit))
				{
					CLOCKElement* element = m_T1Queue.front();
					if (element->referenceBit)
					{
						// Move the head page in T1 to tail position in T1. Set the page reference bit to 0.
						m_T1Queue.pop_front();
						m_T1Queue.push_back(element);

						element->referenceBit = false;

						if (m_T1Queue.size() >= (size_t)Min((unsigned)m_p + 1, m_B1ListSize) && !element->longBit)
						{
							element->longBit = true;
							--m_numShort;
							++m_numLong;
						}
					}
					else
					{
						// Move the head page in T1 to tail position in T2. Set the page reference bit to 0.
						m_T1Queue.pop_front();
						m_T2Queue.push_back(element);

						element->referenceBit = false;
						element->firstList = false;

						m_q = (int)Max(m_q - 1, maxNumElements - m_T1Queue.size());
					}
				}

				// Demote excessive pages
				typename std::deque<CLOCKElement*>::iterator demoteT1 = m_T1Queue.end();
				typename std::deque<CLOCKElement*>::iterator demoteT2 = m_T2Queue.end();

				// Try to find something in T1
				if (m_T1Queue.size() >= (size_t)Max(1u, m_p))
				{
					demoteT1 = m_T1Queue.begin();
					while (demoteT1 != m_T1Queue.end())
					{
						if (GetUsage((*demoteT1)->pageKey, (*demoteT1)->page) > 1)
							++demoteT1;
						else
							break;
					}
				}

				// Demote in T2 if nothing can be deleted in T1
				if (demoteT1 == m_T1Queue.end())
				{
					demoteT2 = m_T2Queue.begin();
					while (demoteT2 != m_T2Queue.end())
					{
						if (GetUsage((*demoteT2)->pageKey, (*demoteT2)->page) > 1)
							++demoteT2;
						else
							break;
					}
				}

				// Perform demoting
				if (demoteT1 != m_T1Queue.end())
				{
					// Demote the head page in T1 and make cacheIt the MRU page in B1. nS = nS - 1
					CLOCKElement* element = *demoteT1;
					m_T1Queue.erase(demoteT1);

					if (element->longBit)
						--m_numLong;
					else
						--m_numShort;

					typename tbb::concurrent_hash_map<KEY, CLOCKElement*>::accessor eraseAccessor;
					bool check = m_T1T2map.find(eraseAccessor, element->pageKey);
					assert(check);

					m_B1List.push_front(element);
					++m_B1ListSize;

					ElementIt elementAndIt; elementAndIt.element = element; elementAndIt.it = m_B1List.begin();
					m_B1B2map.insert(std::make_pair(element->pageKey, elementAndIt));

					DecUsage(element->pageKey, element->page, true);
					m_usedMemory -= m_interface.CacheGetValueSize(element->page);
					m_T1T2map.erase(eraseAccessor);
				}
				else if (demoteT2 != m_T2Queue.end())
				{
					// Demote the head page in T2 and make cacheIt the MRU page in B2. nL = nL - 1
					CLOCKElement* element = *demoteT2;
					m_T2Queue.erase(demoteT2);

					--m_numLong;

					typename tbb::concurrent_hash_map<KEY, CLOCKElement*>::accessor eraseAccessor;
					bool check = m_T1T2map.find(eraseAccessor, element->pageKey);
					assert(check);

					m_B2List.push_front(element);
					++m_B2ListSize;

					ElementIt elementAndIt; elementAndIt.element = element; elementAndIt.it = m_B2List.begin();
					m_B1B2map.insert(std::make_pair(element->pageKey, elementAndIt));

					DecUsage(element->pageKey, element->page, true);
					m_usedMemory -= m_interface.CacheGetValueSize(element->page);
					m_T1T2map.erase(eraseAccessor);
				}

				// History replacement
				auto historyIt = m_B1B2map.find(key);
				if (historyIt == m_B1B2map.end() && (int)m_B1B2map.size() >= maxNumElements + 1)
				{
					if (m_B1ListSize > Max(0, m_q) || m_B2ListSize == 0)
					{
						// Remove the bottom page in B1 from history
						CLOCKElement* toRemove = m_B1List.back();
						m_B1List.pop_back();
						--m_B1ListSize;
						m_B1B2map.erase(toRemove->pageKey);
						delete toRemove;
					}
					else if (m_B2ListSize > 0)
					{
						// Remove the bottom page in B2 from history
						CLOCKElement* toRemove = m_B2List.back();
						m_B2List.pop_back();
						--m_B2ListSize;
						m_B1B2map.erase(toRemove->pageKey);
						delete toRemove;
					}
				}
			}

			auto historyIt = m_B1B2map.find(key);
			if (historyIt == m_B1B2map.end()) // history miss
			{
				// Insert x at the tail of T1
				CLOCKElement* element = new CLOCKElement(key, value);
				element->firstList = true; // pushing to T1

				// Set the page reference bit of x to 0
				element->referenceBit = false;

				// Set filter bit to Short
				element->longBit = false;
				++m_numShort;

				writeAccessor->second = element;
				m_T1Queue.push_back(element);
			}
			else
			{
				ElementIt& elemIt = historyIt->second;

				// History hit
				if (elemIt.element->firstList) // x in B1
				{
					// Adapt: increase the target size for the list T1 as: p = min {p + max {1, ns / B1.size()}, cacheSize}
					m_p = (int)Min(m_p + Max(1, m_B1ListSize ? m_numShort / m_B1ListSize : 0), maxNumElements);

					// Pull value
					elemIt.element->page = value;

					// Set the page reference bit of x to false
					elemIt.element->referenceBit = false;

					// Set type of x to L
					elemIt.element->longBit = true;

					// Set nl = nl + 1
					++m_numLong;

					// Move x to the tail of T1
					writeAccessor->second = elemIt.element;
					m_T1Queue.push_back(elemIt.element);

					m_B1List.erase(elemIt.it);
					--m_B1ListSize;
					m_B1B2map.erase(historyIt);
				}
				else // x in B2
				{
					// Adapt: decrease the target size for the list T1 as: p = max {p - max {1, nl / B2.size()}, 0}
					m_p = Max(m_p - Max(1, m_B2ListSize ? m_numLong / m_B2ListSize : 0), 0);

					// Pull value
					elemIt.element->page = value;
					elemIt.element->firstList = true; // pushing to T1

					// Set the page reference bit of x to false
					elemIt.element->referenceBit = false;

					// Set type of x to L
					elemIt.element->longBit = true;

					// Set nl = nl + 1
					++m_numLong;

					// Move x to the tail of T1
					writeAccessor->second = elemIt.element;
					m_T1Queue.push_back(elemIt.element);

					m_B2List.erase(elemIt.it);
					--m_B2ListSize;
					m_B1B2map.erase(historyIt);

					// Adapt
					if ((int)m_T2Queue.size() + m_B2ListSize + (int)m_T1Queue.size() - m_numShort >= maxNumElements)
					{
						// Set target q = min {q + 1, 2 * cacheSize - T1.size()}
						m_q = (int)Min(m_q + 1, 2 * maxNumElements - m_T1Queue.size());
					}
				}
			}

			m_usedMemory += m_interface.CacheGetValueSize(value);

			return Handle(this, key, value);
		}

		//---------------------------------------------------------------------
		inline void InternalRemove(KEY key)
		{
			typename tbb::concurrent_hash_map<KEY, CLOCKElement*>::accessor writeAccessor;
			if (!m_T1T2map.find(writeAccessor, key))
				return;  // key is not in cache

			// Protect structure with lock
			tbb::queuing_mutex::scoped_lock lock(m_mutex);

			CLOCKElement* element = nullptr;
			if (writeAccessor->second->firstList)
			{
				// Find the key in T1
				auto demoteT1 = m_T1Queue.begin();
				while (demoteT1 != m_T1Queue.end())
				{
					if ((*demoteT1)->pageKey != key)
						++demoteT1;
					else
						break;
				}

				// Remove the key from T1
				element = *demoteT1;
				m_T1Queue.erase(demoteT1);

				if (element->longBit)
					--m_numLong;
				else
					--m_numShort;
			}
			else
			{
				// Find the key in T2
				auto demoteT2 = m_T2Queue.begin();
				while (demoteT2 != m_T2Queue.end())
				{
					if ((*demoteT2)->pageKey != key)
						++demoteT2;
					else
						break;
				}

				// Remove the key from T2
				element = *demoteT2;
				m_T2Queue.erase(demoteT2);

				--m_numLong;
			}

			DecUsage(element->pageKey, element->page);
			m_usedMemory -= m_interface.CacheGetValueSize(element->page);

			delete element;
			m_T1T2map.erase(writeAccessor);
		}

		//---------------------------------------------------------------------
		inline void IncUsage(KEY key, VALUE* value)
		{
			HashMurmur3::hash_t id = HashMurmur3().Set(std::hash<KEY>()(key)).Add(value);
			tbb::concurrent_hash_map<HashMurmur3::hash_t, unsigned int>::accessor acc;
			if (m_usage.find(acc, id))
				++acc->second;
			else
			{
				m_usage.insert(acc, id);
				acc->second = 1;
			}
		}

		//---------------------------------------------------------------------
		inline bool DecUsage(KEY key, VALUE* value, bool waitForLast)
		{
			HashMurmur3::hash_t id = HashMurmur3().Set(std::hash<KEY>()(key)).Add(value);

			int tries = 0; const int max_yield_tries = 100;
			do
			{
				tbb::concurrent_hash_map<HashMurmur3::hash_t, unsigned int>::accessor acc;
				if (m_usage.find(acc, id))
				{
					// Wait for the handle to be released?
					if (waitForLast && acc->second > 1)
					{
						acc.release();

						if (++tries < max_yield_tries)
							continue;

						tbb::this_tbb_thread::yield();
						continue;
					}

					if (--acc->second == 0)
					{
						m_interface.CacheReleaseValue(key, value); // release value
						m_usage.erase(acc);
						return true;
					}
					else
						return false;
				}
				else
				{
					assert(false); // how come?
					return false;
				}
			}
			while (true);
		}

		//---------------------------------------------------------------------
		inline unsigned int GetUsage(KEY key, VALUE* value)
		{
			HashMurmur3::hash_t id = HashMurmur3().Set(std::hash<KEY>()(key)).Add(value);
			tbb::concurrent_hash_map<HashMurmur3::hash_t, unsigned int>::const_accessor acc;
			if (m_usage.find(acc, id))
				return acc->second;
			else
				assert(false); // how come?
			return 0;
		}

	protected:
		struct CLOCKElement
		{
			KEY					pageKey;
			VALUE*				page;
			bool				referenceBit;
			bool				longBit;		// false means Short, true means Long
			bool				firstList;		// element is in T1 or B1 list

			//---------------------------------------------------------------------
			inline CLOCKElement(KEY pageKey, VALUE* page) : pageKey(pageKey), page(page) { }
		};

		int												m_p;			// target size for T1
		int												m_q;			// target size for B1

		tbb::concurrent_hash_map<KEY, CLOCKElement*>	m_T1T2map;
		tbb::queuing_mutex								m_mutex;

		std::deque<CLOCKElement*>						m_T1Queue;
		std::deque<CLOCKElement*>						m_T2Queue;

		int												m_numShort;		// number of Short elements
		int												m_numLong;		// number of Long elements
		tbb::atomic<size_t>								m_usedMemory;

		struct ElementIt { CLOCKElement* element; typename std::list<CLOCKElement*>::iterator it; };
		std::unordered_map<KEY, ElementIt>				m_B1B2map;
		std::list<CLOCKElement*>						m_B1List;
		int												m_B1ListSize;
		std::list<CLOCKElement*>						m_B2List;
		int												m_B2ListSize;

		INTERFACE&										m_interface;
		size_t											m_maxNumElements;
		size_t											m_maxUsedMemory;

		tbb::concurrent_hash_map<HashMurmur3::hash_t, unsigned int> m_usage;	// usage counters for each fetched value
	};
}

namespace std
{
	template <typename KEY, typename USERDATA>
	struct hash<CART::CacheCompoundKey<KEY, USERDATA>>
	{
		size_t operator()(const CART::CacheCompoundKey<KEY, USERDATA>& x) const
		{
			return size_t(x);
		}
	};
}