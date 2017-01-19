/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  The ASF licenses
* this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#ifndef _CS_PTR_HXX_
#define _CS_PTR_HXX_
namespace cornerstone {
    typedef std::atomic<int32> ref_counter_t;

    template<typename T> class CPtr;
    template<typename T> class CPtr<T&>;
    template<typename T> class CPtr<T*>;

    template<typename T, typename ... TArgs>
    CPtr<T> cs_new(TArgs... args) {
        size_t sz = sizeof(ref_counter_t) * 2 + sizeof(T);
        any_ptr p = ::malloc(sz);
        if (p == nilptr) {
            throw std::runtime_error("no memory");
        }

        try {
            ref_counter_t* p_int = new (p) ref_counter_t(0);
            p_int = new (reinterpret_cast<any_ptr>(p_int + 1)) ref_counter_t(0);
            T* p_t = new (reinterpret_cast<any_ptr>(p_int + 1)) T(args...);
            (void)p_t;
        }
        catch (...) {
            ::free(p);
            p = nilptr;
        }

        return CPtr<T>(p);
    }

    template<typename T>
    CPtr<T> cs_alloc(size_t size) {
        size_t sz = size + sizeof(ref_counter_t) * 2;
        any_ptr p = ::malloc(sz);
        if (p == nilptr) {
            throw std::runtime_error("no memory");
        }
        try {
            ref_counter_t* p_int = new (p) ref_counter_t(0);
            p_int = new (reinterpret_cast<any_ptr>(p_int + 1)) ref_counter_t(0);
            (void)p_int;
        }
        catch (...) {
            ::free(p);
            p = nilptr;
        }

        return CPtr<T>(p);
    }

    template<typename T>
    inline CPtr<T> cs_safe(T* t) {
        return CPtr<T>(reinterpret_cast<any_ptr>(reinterpret_cast<ref_counter_t*>(t) - 2));
    }

    template<typename T>
    class CPtr {
    private:
        CPtr(any_ptr p, T* p_t = nilptr)
            : m_AnyPtr(p), m_pType(p_t == nilptr ? reinterpret_cast<T*>(reinterpret_cast<ref_counter_t*>(p) + 2) : p_t) {
            add_ref();
        }
    public:
        CPtr() : m_AnyPtr(nilptr), m_pType(nilptr) {}

        template<typename T1>
        CPtr(const CPtr<T1>& other)
            : m_AnyPtr(other.p_), m_pType(other.m_pType) {
            add_ref();
        }

        CPtr(const CPtr<T>& other)
            : m_AnyPtr(other.m_AnyPtr), m_pType(other.m_pType) {
            add_ref();
        }

        template<typename T1>
        CPtr(CPtr<T1>&& other)
            : m_AnyPtr(other.m_AnyPtr), m_pType(other.m_pType) {
            other.m_AnyPtr = nilptr;
            other.m_pType = nilptr;
        }

        CPtr(CPtr<T>&& other)
            : m_AnyPtr(other.m_AnyPtr), m_pType(other.m_pType) {
            other.m_AnyPtr = nilptr;
            other.m_pType = nilptr;
        }

        ~CPtr() {
            dec_ref_and_free();
        }

        template<typename T1>
        CPtr<T>& operator = (const CPtr<T1>& other) {
            dec_ref_and_free();
			m_AnyPtr = other.m_AnyPtr;
            m_pType = other.m_pType;
            add_ref();
            return *this;
        }

        CPtr<T>& operator = (const CPtr<T>& other) {
            dec_ref_and_free();
            m_AnyPtr = other.m_AnyPtr;
            m_pType = other.m_pType;
            add_ref();
            return *this;
        }

    public:
        inline T* get() const {
            return m_pType;
        }

        inline void reset() {
            dec_ref_and_free();
            m_AnyPtr = nilptr;
            m_pType = nilptr;
        }

        inline T* operator -> () const {
            return get();
        }

        inline T& operator *() const {
            return *get();
        }

        inline operator bool() const {
            return m_AnyPtr != nilptr;
        }

        inline bool operator == (const CPtr<T>& other) const {
            return m_AnyPtr == other.m_AnyPtr;
        }

        inline bool operator != (const CPtr<T>& other) const {
            return m_AnyPtr != other.m_AnyPtr;
        }

        inline bool operator == (const T* p) const {
            if (m_AnyPtr == nilptr) {
                return p == nilptr;
            }

            return get() == p;
        }

        inline bool operator != (const T* p) const {
            if (m_AnyPtr == nilptr) {
                return p != nilptr;
            }

            return get() != p;
        }

    private:
        inline void add_ref() {
            if (m_AnyPtr != nilptr)++ *reinterpret_cast<ref_counter_t*>(m_AnyPtr);
        }

        inline void dec_ref_and_free() {
            if (m_AnyPtr != nilptr && 0 == (-- *reinterpret_cast<ref_counter_t*>(m_AnyPtr))) {
                ++ *(reinterpret_cast<ref_counter_t*>(m_AnyPtr) + 1);
                m_pType->~T();

                // check if there are still references on this, if no, free the memory
                if ((-- *(reinterpret_cast<ref_counter_t*>(m_AnyPtr) + 1)) == 0) {
                    reinterpret_cast<ref_counter_t*>(m_AnyPtr)->~atomic<int32>();
                    (reinterpret_cast<ref_counter_t*>(m_AnyPtr) + 1)->~atomic<int32>();
                    ::free(m_AnyPtr);
                }
            }
        }
    private:
        any_ptr m_AnyPtr;
        T* m_pType;
    public:
        template<typename T1>
        friend class CPtr;
        template<typename T1, typename ... TArgs>
        friend CPtr<T1> cs_new(TArgs... args);
        template<typename T1>
        friend CPtr<T1> cs_safe(T1* t);
        template<typename T1>
        friend CPtr<T1> cs_alloc(size_t size);
    };

    template<typename T>
    class CPtr<T&> {
    public:
        CPtr() : m_AnyPtr(nilptr), m_pType(nilptr) {}

        CPtr(const CPtr<T&>&& other)
            : m_AnyPtr(other.m_AnyPtr), m_pType(other.m_pType) {
            other.m_AnyPtr = nilptr;
            other.m_pType = nilptr;
        }

        template<typename T1>
        CPtr(CPtr<T1&>&& other)
            : m_AnyPtr(other.m_AnyPtr), m_pType(other.m_pType) {
            other.m_AnyPtr = nilptr;
            other.m_pType = nilptr;
        }

        CPtr(const CPtr<T>& src)
            : m_AnyPtr(src.m_AnyPtr), m_pType(src.m_pType) {
            add_ref();
        }

        template<typename T1>
        CPtr(const CPtr<T1>& src)
            : m_AnyPtr(src.m_AnyPtr), m_pType(src.m_pType) {
            add_ref();
        }

        CPtr(const CPtr<T&>& other)
            : m_AnyPtr(other.m_AnyPtr), m_pType(other.m_pType) {
            add_ref();
        }

        template<typename T1>
        CPtr(const CPtr<T1&>& other)
            : m_AnyPtr(other.m_AnyPtr), m_pType(other.m_pType) {
            add_ref();
        }

        ~CPtr() {
            dec_ref_and_free();
        }

        template<typename T1>
        CPtr<T&>& operator = (const CPtr<T1&>& other) {
            dec_ref_and_free();
            m_AnyPtr = other.m_AnyPtr;
            m_pType = other.m_pType;
            add_ref();
            return *this;
        }

        CPtr<T&>& operator = (const CPtr<T&>& other) {
            dec_ref_and_free();
            m_AnyPtr = other.m_AnyPtr;
            m_pType = other.m_pType;
            add_ref();
            return *this;
        }

        template<typename T1>
        CPtr<T&>& operator = (const CPtr<T1>& other) {
            dec_ref_and_free();
            m_AnyPtr = other.m_AnyPtr;
            m_pType = other.m_pType;
            add_ref();
            return *this;
        }

        CPtr<T&>& operator = (const CPtr<T>& other) {
            dec_ref_and_free();
            m_AnyPtr = other.m_AnyPtr;
            m_pType = other.m_pType;
            add_ref();
            return *this;
        }

        inline operator bool() const {
            return m_AnyPtr != nilptr && reinterpret_cast<ref_counter_t*>(m_AnyPtr)->load() > 0;
        }

        inline T& operator *() {
            if (*this) {
                return *get();
            }

            throw std::runtime_error("try to reference to a nilptr");
        }

        inline CPtr<T> operator &() {
            if (*this) {
                return CPtr<T>(m_AnyPtr, m_pType);
            }

            return CPtr<T>();
        }

        inline const T& operator *() const {
            if (*this) {
                return *get();
            }

            throw std::runtime_error("try to reference to a nilptr");
        }

        inline CPtr<T> operator &() const {
            if (*this) {
                return CPtr<T>(m_AnyPtr, m_pType);
            }

            return CPtr<T>();
        }

    private:
        inline T* get() const {
            return m_pType;
        }

        inline void add_ref() {
            if (m_AnyPtr != nilptr)++ *(reinterpret_cast<ref_counter_t*>(m_AnyPtr) + 1);
        }

        inline void dec_ref_and_free() {
            if (m_AnyPtr != nilptr && 0 == (-- *(reinterpret_cast<ref_counter_t*>(m_AnyPtr) + 1))) {
                // check if there are still owners on this, if no, free the memory
                if (reinterpret_cast<ref_counter_t*>(m_AnyPtr)->load() == 0) {
                    reinterpret_cast<ref_counter_t*>(m_AnyPtr)->~atomic<int32>();
                    (reinterpret_cast<ref_counter_t*>(m_AnyPtr) + 1)->~atomic<int32>();
                    ::free(m_AnyPtr);
                }
            }
        }
    private:
        any_ptr m_AnyPtr;
        T* m_pType;
    };
}
#endif //_CS_PTR_HXX_
