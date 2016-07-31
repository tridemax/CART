# C++ CART cache algorithm implementation
Implementation of CART cache algorithm (http://www.cse.iitd.ac.in/~sbansal/pubs/fast04.pdf). Test folder contains comparison with LRU implementation from TBB package. Implementation is fully thread safe - you can use as many threads as you wish to access the cache simultaniously.

Has dependancy on http://threadingbuildingblocks.org. Dependancy on HashMurmur3 can be eliminated by using your favorite hashing function.

#### Usage is fairly straightforward:
* Create small class for creating/deleting values by key (see `struct CacheImpl` in Test.cpp).
* Create cache instance and provide it with this class.
* Use array access overload `cache[key]` to access values inside the cache.

Use provided 'CacheCompoundKey' if you wish to associate some user data with each key.
