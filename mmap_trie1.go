package main

import (
    "fmt"
    "os"
    "syscall"
    "unsafe"
)

type Instance struct {
    nextNode *uint32
    mmap []Node
    mmap_fp *os.File
    mmap_ptr []byte
}

const MaxKeySize = 48
const MaxValueSize = 32

type Node struct {
   flags byte
   ch byte
   child_nodes [36]uint32
}

var sizeOfNode = unsafe.Sizeof(Node{})

func (instance *Instance) add(word []byte) {
   var t *Node = &instance.mmap[0]
   l := len(word)
   for i, ch := range word {
      var id byte
      if ch >= 97 && ch <= 122 {
         id = ch - 87
      } else if ch >= 65 && ch <= 90 {
         id = ch - 55
      } else if ch >= 48 && ch <= 57 {
         id = ch - 48
      } else {
        break
      }

      var loc = t.child_nodes[id]
      if loc == 0 {
         *instance.nextNode += uint32(1)
	 t.child_nodes[id] = *instance.nextNode
	 loc = *instance.nextNode
      }
      t = &instance.mmap[loc]
      t.ch = ch
      if l-1 == i {
        t.flags = t.flags | 0x01
      }
   }
}

func (instance *Instance) find(word []byte) bool {
   var t *Node = &instance.mmap[0]
   for _, ch := range word {
      var id byte
      if ch >= 97 && ch <= 122 {
         id = ch - 87
      } else if ch >= 65 && ch <= 90 {
         id = ch - 55
      } else if ch >= 48 && ch <= 57 {
         id = ch - 48
      } else {
        break
      }

      var loc = t.child_nodes[id]
      if loc == 0 {
         return false
      }
      t = &instance.mmap[loc]
   }
   if t.flags & 0x01 == 0x00 {
      return false
   } else {
      return true
   }
}

func (instance *Instance) init() {
    const maxTrieNode = 1000

    t :=  sizeOfNode * maxTrieNode

    var err error

    instance.mmap_fp, err = os.OpenFile("/tmp/test_trie1.dat", os.O_CREATE|os.O_RDWR, 0666)
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
    instance.nextNode = (*uint32)(unsafe.Pointer(&instance.mmap_ptr[0]))
    mmap := (*[maxTrieNode]Node)(unsafe.Pointer(&instance.mmap_ptr[4]))
    instance.mmap = mmap[:]
}

func (instance *Instance) close() {
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

func main() {
    var instance *Instance = new (Instance)
    instance.init()
    instance.add([]byte("hello"))

    r := instance.find([]byte("hello"))
    fmt.Println(r)

    r = instance.find([]byte("world"))
    fmt.Println(r)

    r = instance.find([]byte("zoo"))
    fmt.Println(r)

    instance.add([]byte("zoo"))
    r = instance.find([]byte("zoo"))
    fmt.Println(r)

    r = instance.find([]byte("hello"))
    fmt.Println(r)

    r = instance.find([]byte("HeLLo"))
    fmt.Println(r)

    r = instance.find([]byte("HeLLo1"))
    fmt.Println(r)

    instance.close()
}
