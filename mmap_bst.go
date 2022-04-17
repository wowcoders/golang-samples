package main

import (
    "fmt"
    "os"
    "bytes"
    "syscall"
    "unsafe"
)

type Instance struct {
    mmap []Node
    mmap_fp *os.File
    mmap_ptr []byte
}

const MaxKeySize = 48
const MaxValueSize = 32

type Node struct {
    flags byte
    key [MaxKeySize]byte
    value [MaxValueSize]byte
}

var sizeOfNode = unsafe.Sizeof(Node{})

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

func (instance *Instance) put(key []byte, value[]byte) {
   key = padOrTrim(key, MaxKeySize)
   nodeIdx := 0
   var t *Node = &instance.mmap[nodeIdx]
   if t.flags & 0x01 == 0x00 {
      t = nil
   }

   fmt.Print("Put:")
   fmt.Println(string(key[:]))

   var m *Node = nil
   for t != nil {
     m = t;

     fmt.Print("\t")
     fmt.Println(string(m.key[:]))

     res := bytes.Compare(key, t.key[:])
     var _nodeIdx = -1
     if res == -1 {
       _nodeIdx = 2 * nodeIdx + 1
     } else if res == 1 {
       _nodeIdx = 2 * nodeIdx + 2
     } else {
       break;
     }

     fmt.Println(_nodeIdx)
     _t := &instance.mmap[_nodeIdx]
     if _t.flags & 0x01 == 0x00 {
        break
     }
     t = _t
     nodeIdx = _nodeIdx
   }

   var node *Node = nil
   if t == nil {
      node = &instance.mmap[0]
   } else {
     res := bytes.Compare(key, m.key[:])
     if res == -1 {
        node = &instance.mmap[2 * nodeIdx + 1]
     } else if res == 1 {
        node = &instance.mmap[2 * nodeIdx + 2]
     } else {
	node = &instance.mmap[nodeIdx]
     }
   }

   copy(node.key[:], key)
   copy(node.value[:], value)
   node.flags = 0x01
}

func (instance *Instance) find(key []byte) *Node {
   key = padOrTrim(key, MaxKeySize)
   nodeIdx := 0
   var t *Node = &instance.mmap[nodeIdx]
   if t.flags & 0x01 == 0x00 {
      t = nil
   }

   fmt.Print("GET:")
   fmt.Println(string(key[:]))

   var m *Node = nil
   for t != nil {
     m = t;

     fmt.Print("\t")
     fmt.Println(string(m.key[:]))

     res := bytes.Compare(key, t.key[:])
     if res == 0 {
       break
     } else if res == -1 {
       nodeIdx = 2 * nodeIdx + 1
     } else {
       nodeIdx = 2 * nodeIdx + 2
     }
     t = &instance.mmap[nodeIdx]
     if t.flags & 0x01 == 0x00 {
        t = nil
        break
     }
   }
   return t
}

func (instance *Instance) get(key []byte) {
   t := instance.find(key)
   if t == nil {
     fmt.Println("--->not found")
   } else {
     fmt.Print("--->found---:")
     fmt.Println(string(t.value[:]))
   }
}

func (instance *Instance) init() {
    const nKeys = 1000

    t :=  sizeOfNode * nKeys

    var err error

    instance.mmap_fp, err = os.OpenFile("/tmp/test.dat", os.O_CREATE|os.O_RDWR, 0666)
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

    mmap := (*[nKeys]Node)(unsafe.Pointer(&instance.mmap_ptr[0]))
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
    instance.put([]byte("jumanji"), []byte("6.9"))
    instance.put([]byte("ace ventura: when nature calls"), []byte("6.1"))
    instance.put([]byte("tom and huck"), []byte("5.4"))
    instance.put([]byte("toy story"), []byte("7.7"))
    instance.put([]byte("balto"), []byte("7.1"))
    instance.put([]byte("grumpier old men"), []byte("6.5"))
    instance.put([]byte("casino"), []byte("7.8"))
    instance.put([]byte("cutthroat island"), []byte("5.7"))
    instance.put([]byte("dracula: dead and loving it"), []byte("5.7"))
    instance.put([]byte("father of the bride part ii"), []byte("5.7"))
    instance.put([]byte("four rooms"), []byte("6.5"))
    instance.put([]byte("goldeneye"), []byte("6.6"))
    instance.put([]byte("money train"), []byte("5.4"))
    instance.put([]byte("nixon"), []byte("7.1"))
    instance.put([]byte("sabrina"), []byte("6.2"))
    instance.put([]byte("sense and sensibility"), []byte("7.2"))
    instance.put([]byte("sudden death"), []byte("5.5"))
    instance.put([]byte("the american president"), []byte("6.5"))
    instance.put([]byte("waiting to exhale"), []byte("6.1"))
    instance.put([]byte("heat"), []byte("7.7"))

    instance.get([]byte("heat"))
    instance.get([]byte("casino"))
    instance.get([]byte("RRR"))
    instance.get([]byte("Beast"))
    instance.get([]byte("KGF"))

    instance.close()
}
