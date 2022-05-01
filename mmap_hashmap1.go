package main

import (
    "fmt"
    "os"
    "syscall"
    "unsafe"
    "hash/fnv"
    "sync"
    "bytes"
)

type LockMap struct {
   mu4map sync.RWMutex
   mu4key map[uint32] *sync.RWMutex
}

func (lockMap *LockMap) getLock(key uint32) * sync.RWMutex {
   lockMap.mu4map.RLock()

   if lock, ok := lockMap.mu4key[key]; ok {
      lockMap.mu4map.RUnlock()
      return lock
   }

   lockMap.mu4map.RUnlock()

   lockMap.mu4map.Lock()
   if lock, ok := lockMap.mu4key[key]; ok {
      lockMap.mu4map.Unlock()
      return lock
   }

   lock := &sync.RWMutex{}

   lockMap.mu4key[key] = lock

   lockMap.mu4map.Unlock()

   return lock
}

type MMap struct {
    mmap_fp *os.File
    mmap_ptr []byte
}

func (instance *MMap) init(size int, filename string) {
    t :=  size

    var err error

    instance.mmap_fp, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    _, err = instance.mmap_fp.Seek(int64(t-1), 0)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    _, err = instance.mmap_fp.Write([]byte(" "))
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    instance.mmap_ptr, err = syscall.Mmap(int(instance.mmap_fp.Fd()), 0, int(t), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

//    mmap := (*[maxTrieNode]Node)(unsafe.Pointer(&instance.mmap_ptr[4]))
//    instance.mmap = mmap[:]
}

func (instance *MMap) close() {
    var err error
    err = syscall.Munmap(instance.mmap_ptr)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    err = instance.mmap_fp.Close()
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
}

const (
    KiB = 1024
    MiB = 1024*1024
    GiB = 1024*1024*1024
)

const MaxValueSize = 128

type ValueFile struct {
   fp *os.File
}

func (this *ValueFile) init(filename string) {
    var err error
    this.fp, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
}

func (this *ValueFile) close() {
    err := this.fp.Close()
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
}

func (this *ValueFile) writeAtEof(value []byte) uint32 {
    var err error
    var offset int64
    offset, err = this.fp.Seek(int64(0), 2)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    value = padOrTrim(value, MaxValueSize)
    _, err = this.fp.Write([]byte(value))
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    return uint32(offset)
}

func (this *ValueFile) readAt(offset uint32) []byte {
    var err error
    _, err = this.fp.Seek(int64(offset), 0)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    value := make([]byte, MaxValueSize)
    _, err = this.fp.Read(value)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    return value
}



const MaxKeySize = 32
type Instance struct {
    locks *LockMap
    numberOfSlots uint32
    mmap_hashtable *MMap
    htSlots []HashtableSlot
    mmap_duplicates *MMap
    mmap_duplicates_next *uint32
    files [256]os.File
    valueFile *ValueFile
}

type IndexBlockHeader struct {
    slots uint16
    used uint16
}

type IdxEntry struct {
    fno_lineno uint32
    key [MaxKeySize]byte
}

type HashtableSlot struct {
    fno_lineno uint32
}

var sizeOfNode = unsafe.Sizeof(HashtableSlot{})
var sizeOfIdxEntry = unsafe.Sizeof(IdxEntry{})
var sizeOfIndexBlockHeader = unsafe.Sizeof(IndexBlockHeader{})

func CreateLockByKey() *LockMap {
    return &LockMap{
       mu4map: sync.RWMutex{},
       mu4key: map[uint32]*sync.RWMutex{},
    }
}

func (instance *Instance) init() {
    instance.locks = CreateLockByKey()
    const hashTableSize = 4 * GiB
    instance.numberOfSlots = uint32(hashTableSize / int(sizeOfNode))

    fmt.Println(instance.numberOfSlots)

    instance.mmap_hashtable = new (MMap);
    instance.mmap_hashtable.init(hashTableSize, "/tmp/hashtable_1.bin")

    mmap := (*[1073741824]HashtableSlot)(unsafe.Pointer(&instance.mmap_hashtable.mmap_ptr[0]))
    instance.htSlots = mmap[:]

    instance.mmap_duplicates = new (MMap);
    instance.mmap_duplicates.init(2 * 1024 * 1024 * 1024, "/tmp/hashtable_duplicates_1.bin")

    instance.mmap_duplicates_next = (*uint32)(unsafe.Pointer(&instance.mmap_duplicates.mmap_ptr[0]))

    if *instance.mmap_duplicates_next == 0 {
       *instance.mmap_duplicates_next = 4
    }

    instance.valueFile = new (ValueFile)

    instance.valueFile.init("/tmp/hashtable_1_1_value.bin")
}

func (instance *Instance) close() {
    instance.mmap_hashtable.close();
}

func padOrTrim(bb []byte, size int) []byte {
    l := len(bb)
    if l == size {
        return bb
    }
    if l > size {
        return bb[l-size:]
    }
    tmp := make([]byte, size)
    copy(tmp[size-l:], bb)
    return tmp
}

func (instance *Instance) put(key []byte, value []byte) {
    h := fnv.New32a()
    h.Write([]byte(key))
    val := h.Sum32()

    idx := val%instance.numberOfSlots

    lock := instance.locks.getLock(idx)

    lock.Lock()

    node := &instance.htSlots[idx]

    key = padOrTrim(key, MaxKeySize)

    if node.fno_lineno == 0 {
      node.fno_lineno = *instance.mmap_duplicates_next//TODO: mul thread support, filemax checkpadOrTrim

      blockHeader := (*IndexBlockHeader)(unsafe.Pointer(&instance.mmap_duplicates.mmap_ptr[node.fno_lineno]))
      blockHeader.slots = 4
      blockHeader.used = 1
      idxEntry := (*IdxEntry)(unsafe.Pointer(&instance.mmap_duplicates.mmap_ptr[node.fno_lineno + uint32(sizeOfIndexBlockHeader)]))
      copy(idxEntry.key[:], key)
      idxEntry.fno_lineno = instance.valueFile.writeAtEof(value)

      *instance.mmap_duplicates_next += uint32(sizeOfIndexBlockHeader + sizeOfIdxEntry * 4)
    } else {
      blockHeader := (*IndexBlockHeader)(unsafe.Pointer(&instance.mmap_duplicates.mmap_ptr[node.fno_lineno]))

      for i := uint16(0); i < blockHeader.used; i++ {
         idxEntry := (*IdxEntry)(unsafe.Pointer(&instance.mmap_duplicates.mmap_ptr[node.fno_lineno + uint32(sizeOfIndexBlockHeader) + uint32(i)]))
	 fmt.Println(string(idxEntry.key[:]))
	 res := bytes.Compare(key, idxEntry.key[:])
	 if res == 0 {
           fmt.Println("Value Found")
	   fmt.Println(idxEntry.fno_lineno)
	   fmt.Println(string(instance.valueFile.readAt(idxEntry.fno_lineno)))
           idxEntry.fno_lineno = instance.valueFile.writeAtEof(value)
	 }
      }
    }

    //duplicate handling
    //value inserts

    lock.Unlock()
}

func (instance *Instance) hasKey(key []byte) bool {
    h := fnv.New32a()
    h.Write([]byte(key))
    val := h.Sum32()
    idx := val%instance.numberOfSlots

    node := &instance.htSlots[idx]

    key = padOrTrim(key, MaxKeySize)

    if node.fno_lineno == 0 {
       return false
    } else {
      lock := instance.locks.getLock(idx)
      lock.RLock()
      blockHeader := (*IndexBlockHeader)(unsafe.Pointer(&instance.mmap_duplicates.mmap_ptr[node.fno_lineno]))
      for i := uint16(0); i < blockHeader.used; i++ {
         idxEntry := (*IdxEntry)(unsafe.Pointer(&instance.mmap_duplicates.mmap_ptr[node.fno_lineno + uint32(sizeOfIndexBlockHeader) + uint32(i)]))
	 fmt.Println(string(idxEntry.key[:]))
	 res := bytes.Compare(key, idxEntry.key[:])
	 if res == 0 {
            lock.RUnlock()
            return true
         }
      }
      lock.RUnlock()
      return false
    }
}

func (instance *Instance) get(value []byte) []byte {
    return nil
}

func main() {
    var instance *Instance = new (Instance)
    instance.init()

    instance.put([]byte("key"), []byte("value"))
    instance.put([]byte("host"), []byte("vpc3848"))
    instance.put([]byte("host1"), []byte("vpc3848"))

    r := instance.hasKey([]byte("key"))
    fmt.Println(r)

    r = instance.hasKey([]byte("unknown"))
    fmt.Println(r)

    instance.close()
}
