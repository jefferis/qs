#include <Rcpp.h>
#include <fstream>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <memory>
#include <array>
#include "RApiSerializeAPI.h"
#include "zstd.h"

#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

// Make unique function for c++11 (from c++14)
// https://stackoverflow.com/questions/17902405/how-to-implement-make-unique-function-in-c11
// this is kind of dangerous to add directly into std namespace, dont do this if compiling with c++14
namespace std {
  template<class T> struct _Unique_if {
    typedef unique_ptr<T> _Single_object;
  };
  
  template<class T> struct _Unique_if<T[]> {
    typedef unique_ptr<T[]> _Unknown_bound;
  };
  
  template<class T, size_t N> struct _Unique_if<T[N]> {
    typedef void _Known_bound;
  };
  
  template<class T, class... Args>
  typename _Unique_if<T>::_Single_object
    make_unique(Args&&... args) {
      return unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
  
  template<class T>
  typename _Unique_if<T>::_Unknown_bound
    make_unique(size_t n) {
      typedef typename remove_extent<T>::type U;
      return unique_ptr<T>(new U[n]());
    }
  
  template<class T, class... Args>
  typename _Unique_if<T>::_Known_bound
    make_unique(Args&&...) = delete;
}

////////////////////////////////////////////////////////////////
// common utility functions and constants
////////////////////////////////////////////////////////////////

using namespace Rcpp;
static bool show_warnings = true;

// #define BLOCKSIZE 262144 // 2^20 bytes per block
#define BLOCKRESERVE 64
#define NA_STRING_LENGTH 4294967295 // 2^32-1 -- length used to signify NA value

static size_t BLOCKSIZE = 524288;

static const unsigned char list_header_5 = 0x20; 
static const unsigned char list_header_8 = 0x01;
static const unsigned char list_header_16 = 0x02;
static const unsigned char list_header_32 = 0x03;
static const unsigned char list_header_64 = 0x04;

static const unsigned char numeric_header_5 = 0x40; 
static const unsigned char numeric_header_8 = 0x05;
static const unsigned char numeric_header_16 = 0x06;
static const unsigned char numeric_header_32 = 0x07;
static const unsigned char numeric_header_64 = 0x08;

static const unsigned char integer_header_5 = 0x60; 
static const unsigned char integer_header_8 = 0x09;
static const unsigned char integer_header_16 = 0x0A;
static const unsigned char integer_header_32 = 0x0B;
static const unsigned char integer_header_64 = 0x0C;

static const unsigned char logical_header_5 = 0x80; 
static const unsigned char logical_header_8 = 0x0D;
static const unsigned char logical_header_16 = 0x0E;
static const unsigned char logical_header_32 = 0x0F;
static const unsigned char logical_header_64 = 0x10;

static const unsigned char raw_header_32 = 0x17;
static const unsigned char raw_header_64 = 0x18;

static const unsigned char null_header = 0x00; 

static const unsigned char character_header_5 = 0xA0; 
static const unsigned char character_header_8 = 0x11;
static const unsigned char character_header_16 = 0x12;
static const unsigned char character_header_32 = 0x13;
static const unsigned char character_header_64 = 0x14;

static const unsigned char string_header_NA = 0x0F;
static const unsigned char string_header_5 = 0x20; 
static const unsigned char string_header_8 = 0x01;
static const unsigned char string_header_16 = 0x02;
static const unsigned char string_header_32 = 0x03;

static const unsigned char string_enc_native = 0x00; 
static const unsigned char string_enc_utf8 = 0x40;
static const unsigned char string_enc_latin1 = 0x80;
static const unsigned char string_enc_bytes = 0xC0;

static const unsigned char attribute_header_5 = 0xE0;
static const unsigned char attribute_header_8 = 0x1E;
static const unsigned char attribute_header_32 = 0x1F;

static const unsigned char complex_header_32 = 0x15;
static const unsigned char complex_header_64 = 0x16;

static const unsigned char nstype_header_32 = 0x19;
static const unsigned char nstype_header_64 = 0x1A;

static const std::set<SEXPTYPE> stypes = {REALSXP, INTSXP, LGLSXP, STRSXP, CHARSXP, NILSXP, VECSXP, CPLXSXP, RAWSXP};

// https://stackoverflow.com/questions/22346369/initialize-integer-literal-to-stdsize-t/22346540#22346540
constexpr std::size_t intToSize ( unsigned long long n ) { return n; }

inline void writeSizeToFile8(std::ofstream & myFile, size_t x) {uint64_t x_temp = static_cast<uint64_t>(x); myFile.write(reinterpret_cast<char*>(&x_temp),8);}
inline void writeSizeToFile4(std::ofstream & myFile, size_t x) {uint32_t x_temp = static_cast<uint32_t>(x); myFile.write(reinterpret_cast<char*>(&x_temp),4);}
size_t readSizeFromFile4(std::ifstream & myFile) {
  std::array<char,4> a = {0,0,0,0};
  myFile.read(a.data(),4);
  return *reinterpret_cast<uint32_t*>(a.data());
}
size_t readSizeFromFile8(std::ifstream & myFile) {
  std::array<char,8> a = {0,0,0,0,0,0,0,0};
  myFile.read(a.data(),8);
  return *reinterpret_cast<uint64_t*>(a.data());
}
uint32_t char2Size4(std::array<char,4> a) { return *reinterpret_cast<uint32_t*>(a.data()); }
uint64_t char2Size8(std::array<char,8> a) { return *reinterpret_cast<uint64_t*>(a.data()); }

////////////////////////////////////////////////////////////////
// de-serialization functions
////////////////////////////////////////////////////////////////


void readHeader(char* header, SEXPTYPE & object_type, size_t & r_array_len, size_t & data_offset) {
  unsigned char h5 = reinterpret_cast<unsigned char*>(header)[data_offset] & 0xE0;
  switch(h5) {
  case numeric_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = REALSXP;
    return;
  case list_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = VECSXP;
    return;
  case integer_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = INTSXP;
    return;
  case logical_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = LGLSXP;
    return;
  case character_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = STRSXP;
    return;
  case attribute_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = ANYSXP;
    return;
  }
  unsigned char hd = reinterpret_cast<unsigned char*>(header)[data_offset];
  switch(hd) {
  case numeric_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = REALSXP;
    return;
  case numeric_header_16:
    r_array_len = *reinterpret_cast<uint16_t*>(header+data_offset+1) ;
    data_offset += 3;
    object_type = REALSXP;
    return;
  case numeric_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = REALSXP;
    return;
  case numeric_header_64:
    r_array_len =  *reinterpret_cast<uint64_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = REALSXP;
    return;
  case list_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = VECSXP;
    return;
  case list_header_16:
    r_array_len = *reinterpret_cast<uint16_t*>(header+data_offset+1) ;
    data_offset += 3;
    object_type = VECSXP;
    return;
  case list_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = VECSXP;
    return;
  case list_header_64:
    r_array_len =  *reinterpret_cast<uint64_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = VECSXP;
    return;
  case integer_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = INTSXP;
    return;
  case integer_header_16:
    r_array_len = *reinterpret_cast<uint16_t*>(header+data_offset+1) ;
    data_offset += 3;
    object_type = INTSXP;
    return;
  case integer_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = INTSXP;
    return;
  case integer_header_64:
    r_array_len =  *reinterpret_cast<uint64_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = INTSXP;
    return;
  case logical_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = LGLSXP;
    return;
  case logical_header_16:
    r_array_len = *reinterpret_cast<uint16_t*>(header+data_offset+1) ;
    data_offset += 3;
    object_type = LGLSXP;
    return;
  case logical_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = LGLSXP;
    return;
  case logical_header_64:
    r_array_len =  *reinterpret_cast<uint64_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = LGLSXP;
    return;
  case raw_header_32:
    r_array_len = *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = RAWSXP;
    return;
  case raw_header_64:
    r_array_len =  *reinterpret_cast<uint64_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = RAWSXP;
    return;
  case character_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = STRSXP;
    return;
  case character_header_16:
    r_array_len = *reinterpret_cast<uint16_t*>(header+data_offset+1) ;
    data_offset += 3;
    object_type = STRSXP;
    return;
  case character_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = STRSXP;
    return;
  case character_header_64:
    r_array_len =  *reinterpret_cast<uint64_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = STRSXP;
    return;
  case complex_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = CPLXSXP;
    return;
  case complex_header_64:
    r_array_len =  *reinterpret_cast<uint64_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = CPLXSXP;
    return;
  case null_header:
    r_array_len =  0;
    data_offset += 1;
    object_type = NILSXP;
    return;
  case attribute_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = ANYSXP;
    return;
  case attribute_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = ANYSXP;
    return;
  case nstype_header_32:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 5;
    object_type = S4SXP;
    return;
  case nstype_header_64:
    r_array_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
    data_offset += 9;
    object_type = S4SXP;
    return;
  }
  // additional types
  throw exception("something went wrong (reading object header)");
}


void readStringHeader(char* header, uint32_t & r_string_len, cetype_t & ce_enc, size_t & data_offset) {
  unsigned char enc = reinterpret_cast<unsigned char*>(header)[data_offset] & 0xC0;
  switch(enc) {
  case string_enc_native:
    ce_enc = CE_NATIVE; break;
  case string_enc_utf8:
    ce_enc = CE_UTF8; break;
  case string_enc_latin1:
    ce_enc = CE_LATIN1; break;
  case string_enc_bytes:
    ce_enc = CE_BYTES; break;
  }
  
  if((reinterpret_cast<unsigned char*>(header)[data_offset] & 0x20) == string_header_5) {
    r_string_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    return;
  } else {
    unsigned char hd = reinterpret_cast<unsigned char*>(header)[data_offset] & 0x1F;
    switch(hd) {
    case string_header_8:
      r_string_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
      data_offset += 2;
      return;
    case string_header_16:
      r_string_len = *reinterpret_cast<uint16_t*>(header+data_offset+1) ;
      data_offset += 3;
      return;
    case string_header_32:
      r_string_len =  *reinterpret_cast<uint32_t*>(header+data_offset+1) ;
      data_offset += 5;
      return;
    case string_header_NA:
      r_string_len = NA_STRING_LENGTH;
      data_offset += 1;
      return;
    }
  } 
  throw exception("something went wrong (reading string header)");
}

RObject createRObject(SEXPTYPE type, size_t length) {
  RObject obj;
  switch(type) {
  case VECSXP: 
    obj = List(length);
    // obj = Rcpp::Vector<VECSXP>(length) ;
    break;
  case REALSXP:
    obj = NumericVector(length);
    break;
  case INTSXP:
    obj = IntegerVector(length);
    break;
  case LGLSXP:
    obj = LogicalVector(length);
    break;
  case RAWSXP:
    obj = RawVector(length);
    break;
  case STRSXP:
    obj = CharacterVector(length);
    break;
  case CPLXSXP:
    obj = ComplexVector(length);
    break;
  case NILSXP:
    obj = R_NilValue;
  }
  return obj;
}


// usage of shared_pts and weak_ptrs reference
// https://thispointer.com/shared_ptr-binary-trees-and-the-problem-of-cyclic-references/
// How I think this should work:
// 1) Root node is a shared/unique ptr
// 2) Children are added to parent node (when parent is a list), and parent node owns all children
// 3) Recursively, root owns all children and subsequent children
// 4) All external references should not own any node -- therefore, use raw pointer for external use outside of class
//  Child nodes -- lists elements
//  Attribute nodes -- attributes
struct ObjNode {
  size_t remaining_children = 0;
  size_t remaining_attributes = 0;
  ObjNode * parent;
  std::vector< std::unique_ptr<ObjNode> > attributes;
  std::vector< std::string > attributes_names;
  std::vector< std::unique_ptr<ObjNode> > children; // children that need to be referenced later, not all of them
  RObject obj;
  ObjNode(SEXPTYPE type, size_t length, size_t attr_length=0) {
    switch(type) {
    case VECSXP: 
      obj = List(length);
      remaining_children = length;
      break;
    case REALSXP:
      obj = NumericVector(length);
      break;
    case INTSXP:
      obj = IntegerVector(length);
      break;
    case LGLSXP:
      obj = LogicalVector(length);
      break;
    case RAWSXP:
      obj = RawVector(length);
      break;
    case STRSXP:
      obj = CharacterVector(length);
      break;
    case CPLXSXP:
      obj = ComplexVector(length);
      break;
    case NILSXP:
      obj = R_NilValue;
    }
    remaining_attributes = attr_length;
  }
  ObjNode() {}
};


struct String_Future {
  RObject parentObj;
  size_t position;
  std::string fstring;
  cetype_t enc;
  String_Future(RObject pObj, size_t pos, int l, cetype_t e) {
    parentObj = pObj;
    position = pos;
    fstring = std::string(l, '\0');
    enc = e;
  }
  void push_string() {
    //parentObj[position] = Rf_mkCharLenCE(fstring.data(), fstring.size(), enc);
    SET_STRING_ELT(parentObj, position, Rf_mkCharLenCE(fstring.data(), fstring.size(), enc));
  }
  char* dataPtr() {
    return &fstring[0];
  }
};


struct NsObj_Future {
  ObjNode * node;
  size_t position;
  bool is_attribute;
  RawVector data;
  NsObj_Future(ObjNode * no, size_t data_len, size_t pos, bool is_att) {
    node = no;
    position = pos;
    is_attribute = is_att;
    data = RawVector(data_len);
  }
  void push() {
    node->obj = unserializeFromRaw(data);
    if(!is_attribute && node->parent->parent != node->parent) { // not root and not attribute, then insert to list
      SET_VECTOR_ELT(node->parent->obj, position, node->obj);
    }
  }
  char* dataPtr() {
    return reinterpret_cast<char*>(RAW(data));
  }
};


RObject addNewNodeToParent(ObjNode *& parent, SEXPTYPE type, size_t length, size_t attr_length) {
  if( parent->parent == parent ) { // root
    std::unique_ptr<ObjNode> child = std::make_unique<ObjNode>(type, length, attr_length);
    child->parent = parent;
    parent->children.push_back(std::move(child));
    parent = parent->children.back().get();
    return parent->obj;
  }
  
  // std::cout << type << " " << length << " " << attr_length << " new node \n";
  // std::cout << TYPEOF(parent->obj) << " " << parent->remaining_children << " " << parent->remaining_attributes << " parent \n";
  if(parent->remaining_attributes == 0 && parent->remaining_children == 0) throw exception("too many serialized objects in attributes or list");
  
  RObject return_obj;
  if(parent->remaining_children > 0) { 
    if(attr_length == 0 && (type != VECSXP || length == 0)) { // leaf node are objects with no attributes/children
      return_obj = createRObject(type, length);
      SET_VECTOR_ELT(parent->obj, Rf_xlength(parent->obj) - parent->remaining_children, return_obj);
      parent->remaining_children--;
      
      while(parent->remaining_attributes == 0 && parent->remaining_children == 0) {
        parent = parent->parent;
        // std::cout << "down\n";
        if( parent->parent == parent ) break; // root
      }
    } else {
      std::unique_ptr<ObjNode> child = std::make_unique<ObjNode>(type, length, attr_length);
      child->parent = parent;
      return_obj = child->obj;
      parent->children.push_back(std::move(child));
      SET_VECTOR_ELT(parent->obj, Rf_xlength(parent->obj) - parent->remaining_children, parent->children.back()->obj);
      parent->remaining_children--;
      
      parent = parent->children.back().get();
      // std::cout << "up\n";
      // std::cout << TYPEOF(parent->obj) << " " << parent->remaining_children << " " << parent->remaining_attributes << " parent \n";
    }
  } else { // if parent->remaining_attributes > 0
    std::unique_ptr<ObjNode> child = std::make_unique<ObjNode>(type, length, attr_length);
    child->parent = parent;
    return_obj = child->obj;
    parent->attributes.push_back(std::move(child));
    parent->remaining_attributes--;
    
    if(attr_length > 0 || (type == VECSXP && length > 0)) {
      parent = parent->attributes.back().get();
      // std::cout << "up\n";
      // std::cout << TYPEOF(parent->obj) << " " << parent->remaining_children << " " << parent->remaining_attributes << " parent \n";
    } else {
      while(parent->remaining_attributes == 0 && parent->remaining_children == 0) {
        parent = parent->parent;
        // std::cout << "down\n";
        if( parent->parent == parent ) break; // root
      }
    }
  }
  return return_obj;
}


NsObj_Future addNewNsNodeToParent(ObjNode *& parent, size_t length) {
  std::unique_ptr<ObjNode> child = std::make_unique<ObjNode>();
  child->parent = parent;
  child->obj = R_NilValue; // placeholder
  if( parent->parent == parent ) { // root
    parent->children.push_back(std::move(child));
    return NsObj_Future(parent->children.back().get(), length, 0, false);
  }
  
  if(parent->remaining_attributes == 0 && parent->remaining_children == 0) throw exception("too many serialized objects in attributes or list");
  if(parent->remaining_children > 0) { 
    parent->children.push_back(std::move(child));
    NsObj_Future return_obj = NsObj_Future(parent->children.back().get(), length, Rf_xlength(parent->obj) - parent->remaining_children, false);
    parent->remaining_children--;
    while(parent->remaining_attributes == 0 && parent->remaining_children == 0) {
      parent = parent->parent;
      if( parent->parent == parent ) break; // root
    }
    return return_obj;
  } else { // if parent->remaining_attributes > 0
    parent->attributes.push_back(std::move(child));
    NsObj_Future return_obj = NsObj_Future(parent->attributes.back().get(), length, 0, true);
    parent->remaining_attributes--;
    while(parent->remaining_attributes == 0 && parent->remaining_children == 0) {
      parent = parent->parent;
      if( parent->parent == parent ) break; // root
    }
    return return_obj;
  }
}


char * getObjDataPointer(RObject & obj, SEXPTYPE obj_type, size_t & type_char_width) {
  char* outp;
  switch(obj_type) {
  case REALSXP: 
    outp = reinterpret_cast<char*>(REAL(obj));
    type_char_width = 8;
    break;
  case INTSXP: 
    outp = reinterpret_cast<char*>(INTEGER(obj));
    type_char_width = 4;
    break;
  case LGLSXP: 
    outp = reinterpret_cast<char*>(LOGICAL(obj));
    type_char_width = 4;
    break;
  case RAWSXP: 
    outp = reinterpret_cast<char*>(RAW(obj));
    type_char_width = 1;
    break;
  case CPLXSXP: 
    outp = reinterpret_cast<char*>(COMPLEX(obj));
    type_char_width = 16;
    break;
  default: // should never reach
    throw exception("pointer of non-atomic object requested");
  outp = nullptr; 
  }
  return outp;
}

void setBlockPointers(char* outp, std::vector<char> & block, size_t & data_offset,
                      std::vector< std::pair<char*, size_t> > & block_pointers,
                      size_t block_size, size_t r_array_len, size_t type_char_width, size_t i) {
  size_t next_block = i;
  if(block_size > data_offset) memcpy(outp,block.data()+data_offset, block_size - data_offset);
  size_t bytes_accounted = block_size - data_offset;
  while(bytes_accounted < (r_array_len * type_char_width) ) {
    next_block++;
    if(next_block >= block_pointers.size()) throw exception("reading out of file range");
    block_pointers[next_block].first = outp + bytes_accounted;
    size_t next_bytes = std::min(r_array_len * type_char_width - bytes_accounted, intToSize(BLOCKSIZE));
    block_pointers[next_block].second = next_bytes;
    bytes_accounted += next_bytes;
  }
  data_offset = block_size;
  return;
}

////////////////////////////////////////////////////////////////
// serialization functions
////////////////////////////////////////////////////////////////

struct CompressBuffer {
  size_t number_of_blocks = 0;
  std::vector<char> block = std::vector<char>(BLOCKSIZE);
  std::vector<char> zblock = std::vector<char>(ZSTD_compressBound(BLOCKSIZE));
  size_t current_blocksize=0;
  std::ofstream & myFile;
  size_t compress_level;
  CompressBuffer(std::ofstream & f, int cl) : myFile(f), compress_level(cl) {}
  void flush() {
    if(current_blocksize > 0) {
      size_t zsize = ZSTD_compress(zblock.data(), zblock.size(), block.data(), current_blocksize, compress_level);
      writeSizeToFile4(myFile, zsize);
      myFile.write(zblock.data(), zsize);
      current_blocksize = 0;
      number_of_blocks++;
    }
  }
  void append(char* data, size_t len, bool contiguous = false) {
    size_t current_pointer_consumed = 0;
    while(current_pointer_consumed < len) {
      if( (current_blocksize == BLOCKSIZE) || ((BLOCKSIZE - current_blocksize < BLOCKRESERVE) && !contiguous) ) {
        flush();
      }
      if(current_blocksize == 0 && len - current_pointer_consumed >= BLOCKSIZE) {
        size_t zsize = ZSTD_compress(zblock.data(), zblock.size(), data + current_pointer_consumed, BLOCKSIZE, compress_level);
        writeSizeToFile4(myFile, zsize);
        myFile.write(zblock.data(), zsize);
        current_pointer_consumed += BLOCKSIZE;
        number_of_blocks++;
      } else {
        size_t remaining_pointer_available = len - current_pointer_consumed;
        size_t add_length = remaining_pointer_available < (BLOCKSIZE - current_blocksize) ? remaining_pointer_available : BLOCKSIZE-current_blocksize;
        memcpy(block.data() + current_blocksize, data + current_pointer_consumed, add_length);
        current_blocksize += add_length;
        current_pointer_consumed += add_length;
      }
    }
  }
};

// should this be a member function?
template<typename POD>
inline void vbuf_append_pod(CompressBuffer & vbuf, POD pod, bool contiguous = false) {
  vbuf.append(reinterpret_cast<char*>(&pod), sizeof(pod), contiguous);
}

// write header to vbuf
void writeHeader(CompressBuffer & vbuf, SEXPTYPE object_type, size_t length) {
  switch(object_type) {
  case REALSXP:
    if(length < 32) {
      vbuf_append_pod(vbuf, static_cast<unsigned char>( numeric_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_8)), 1);
      vbuf_append_pod(vbuf, static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_16)), 1);
      vbuf_append_pod(vbuf, static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(length), true );
    }
    return;
  case VECSXP:
    if(length < 32) {
      vbuf_append_pod(vbuf, static_cast<unsigned char>( list_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_8)), 1);
      vbuf_append_pod(vbuf, static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_16)), 1);
      vbuf_append_pod(vbuf, static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(length), true );
    }
    return;
  case INTSXP:
    if(length < 32) {
      vbuf_append_pod(vbuf, static_cast<unsigned char>( integer_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_8)), 1);
      vbuf_append_pod(vbuf, static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_16)), 1);
      vbuf_append_pod(vbuf, static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(length), true );
    }
    return;
  case LGLSXP:
    if(length < 32) {
      vbuf_append_pod(vbuf, static_cast<unsigned char>( logical_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_8)), 1);
      vbuf_append_pod(vbuf, static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_16)), 1);
      vbuf_append_pod(vbuf, static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(length), true );
    }
    return;
  case RAWSXP:
    if(length < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(length), true );
    }
    return;
  case STRSXP:
    if(length < 32) {
      vbuf_append_pod(vbuf, static_cast<unsigned char>( character_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_8)), 1);
      vbuf_append_pod(vbuf, static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_16)), 1);
      vbuf_append_pod(vbuf, static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(length), true );
    }
    return;
  case CPLXSXP:
    if(length < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(length), true );
    }
    return;
  case NILSXP:
    vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&null_header)), 1);
    return;
  default:
    // should never reach here
    throw exception("something went wrong writing object header");
    // vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&null_header)), 1);
    // return;
  }
}

void writeStringHeader(CompressBuffer & vbuf, size_t length, cetype_t ce_enc) {
  unsigned char enc;
  switch(ce_enc) {
  case CE_NATIVE:
    enc = string_enc_native; break;
  case CE_UTF8:
    enc = string_enc_utf8; break;
  case CE_LATIN1:
    enc = string_enc_latin1; break;
  case CE_BYTES:
    enc = string_enc_bytes; break;
  default:
    enc = string_enc_native;
  }
  if(length < 32) {
    vbuf_append_pod(vbuf, static_cast<unsigned char>( string_header_5 | static_cast<unsigned char>(enc) | static_cast<unsigned char>(length) ) );
  } else if(length < 256) {
    vbuf_append_pod(vbuf, static_cast<unsigned char>( string_header_8 | static_cast<unsigned char>(enc) ) );
    vbuf_append_pod(vbuf, static_cast<uint8_t>(length), true );
  } else if(length < 65536) {
    vbuf_append_pod(vbuf, static_cast<unsigned char>( string_header_16 | static_cast<unsigned char>(enc) ) );
    vbuf_append_pod(vbuf, static_cast<uint16_t>(length), true );
  } else {
    vbuf_append_pod(vbuf, static_cast<unsigned char>( string_header_32 | static_cast<unsigned char>(enc) ) );
    vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
  }
}

void writeAttributeHeader(CompressBuffer & vbuf, size_t length) {
  if(length < 32) {
    vbuf_append_pod(vbuf, static_cast<unsigned char>( attribute_header_5 | static_cast<unsigned char>(length) ) );
  } else if(length < 256) {
    vbuf_append_pod(vbuf, static_cast<unsigned char>( attribute_header_8 ) );
    vbuf_append_pod(vbuf, static_cast<uint8_t>(length), true );
  } else {
    vbuf_append_pod(vbuf, static_cast<unsigned char>( attribute_header_32 ) );
    vbuf_append_pod(vbuf, static_cast<uint32_t>(length), true );
  }
}



void appendToVbuf(CompressBuffer & vbuf, RObject & x, bool attributes_processed = false) {
  if(!attributes_processed && stypes.find(TYPEOF(x)) != stypes.end()) {
    std::vector<std::string> anames = x.attributeNames();
    if(anames.size() != 0) {
      writeAttributeHeader(vbuf, anames.size());
      appendToVbuf(vbuf, x, true);
      for(size_t i=0; i<anames.size(); i++) {
        writeStringHeader(vbuf, anames[i].size(),CE_NATIVE);
        vbuf.append(&anames[i][0], anames[i].size(), true);
        RObject xa = x.attr(anames[i]);
        appendToVbuf(vbuf, xa);
      }
    } else {
      appendToVbuf(vbuf, x, true);
    }
  } else if(TYPEOF(x) == STRSXP) {
    size_t dl = Rf_xlength(x);
    writeHeader(vbuf, STRSXP, dl);
    CharacterVector xc = CharacterVector(x);
    for(size_t i=0; i<dl; i++) {
      SEXP xi = xc[i];
      if(xi == NA_STRING) {
        vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&string_header_NA)), 1);
      } else {
        size_t dl = LENGTH(xi);
        writeStringHeader(vbuf, dl, Rf_getCharCE(xi));
        vbuf.append(const_cast<char*>(CHAR(xi)), dl, true);
      }
    }
  } else if(stypes.find(TYPEOF(x)) != stypes.end()) {
    size_t dl = Rf_xlength(x);
    writeHeader(vbuf, TYPEOF(x), dl);
    if(TYPEOF(x) == VECSXP) {
      List xl = List(x);
      for(size_t i=0; i<dl; i++) {
        RObject xi = xl[i];
        appendToVbuf(vbuf, xi);
      }
    } else {
      switch(TYPEOF(x)) {
      case REALSXP:
        vbuf.append(reinterpret_cast<char*>(REAL(x)), dl*8, true); break;
      case INTSXP:
        vbuf.append(reinterpret_cast<char*>(INTEGER(x)), dl*4, true); break;
      case LGLSXP:
        vbuf.append(reinterpret_cast<char*>(LOGICAL(x)), dl*4, true); break;
      case RAWSXP:
        vbuf.append(reinterpret_cast<char*>(RAW(x)), dl, true); break;
      case CPLXSXP:
        vbuf.append(reinterpret_cast<char*>(COMPLEX(x)), dl*16, true); break;
      case NILSXP:
        break;
      }
    }
  } else { // other non-supported SEXPTYPEs use the built in R serialization method
    RawVector xserialized = serializeToRaw(x);
    if(xserialized.size() < 4294967296) {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&nstype_header_32)), 1);
      vbuf_append_pod(vbuf, static_cast<uint32_t>(xserialized.size()), true );
    } else {
      vbuf.append(reinterpret_cast<char*>(const_cast<unsigned char*>(&nstype_header_64)), 1);
      vbuf_append_pod(vbuf, static_cast<uint64_t>(xserialized.size()), true );
    }
    vbuf.append(reinterpret_cast<char*>(RAW(xserialized)), xserialized.size(), true);
  }
}
