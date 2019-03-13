/* qs - Quick Serialization of R Objects
 Copyright (C) 2019-prsent Travers Ching
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.
 
 You can contact the author at:
 https://github.com/traversc/qs
 */


#include <Rcpp.h>

#include <fstream>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <memory>
#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <string>
#include <vector>
#include <climits>

#include <atomic>
#include <thread>
#include <condition_variable>

#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>

#include "RApiSerializeAPI.h"
#include "zstd.h"
#include "lz4.h"
#include "BLOSC/shuffle_routines.h"
#include "BLOSC/unshuffle_routines.h"

#include <R_ext/Rdynload.h>

using namespace Rcpp;

////////////////////////////////////////////////////////////////
// alt rep string class
////////////////////////////////////////////////////////////////

// load alt-rep header -- see altrepisode package by Romain Francois
#if R_VERSION < R_Version(3, 6, 0)
#define class klass
extern "C" {
#include <R_ext/Altrep.h>
}
#undef class
#else
#include <R_ext/Altrep.h>
#endif


struct stdvec_data {
  std::vector<std::string> strings;
  std::vector<unsigned char> encodings;
  R_xlen_t vec_size; 
  stdvec_data(uint64_t N) {
    strings = std::vector<std::string>(N);
    encodings = std::vector<unsigned char>(N);
    vec_size = N;
  }
};

// instead of defining a set of free functions, we structure them
// together in a struct
struct stdvec_string {
  static R_altrep_class_t class_t;
  static SEXP Make(stdvec_data* data, bool owner){
    SEXP xp = PROTECT(R_MakeExternalPtr(data, R_NilValue, R_NilValue));
    if (owner) {
      R_RegisterCFinalizerEx(xp, stdvec_string::Finalize, TRUE);
    }
    SEXP res = R_new_altrep(class_t, xp, R_NilValue);
    UNPROTECT(1);
    return res;
  }
  
  // finalizer for the external pointer
  static void Finalize(SEXP xp){
    delete static_cast<stdvec_data*>(R_ExternalPtrAddr(xp));
  }
  
  // get the std::vector<string>* from the altrep object `x`
  static stdvec_data* Ptr(SEXP vec) {
    return static_cast<stdvec_data*>(R_ExternalPtrAddr(R_altrep_data1(vec)));
  }
  
  // same, but as a reference, for convenience
  static stdvec_data& Get(SEXP vec) {
    return *static_cast<stdvec_data*>(R_ExternalPtrAddr(R_altrep_data1(vec)));
  }
  
  // ALTREP methods -------------------
  
  // The length of the object
  static R_xlen_t Length(SEXP vec){
    return Get(vec).vec_size;
  }
  
  // What gets printed when .Internal(inspect()) is used
  static Rboolean Inspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int)){
    Rprintf("qs alt-rep stdvec_string (len=%d, ptr=%p)\n", Length(x), Ptr(x));
    return TRUE;
  }
  
  // ALTVEC methods ------------------
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return std::move(data2);
    }
    R_xlen_t n = Length(vec);
    data2 = PROTECT(Rf_allocVector(STRSXP, n));
    
    auto data1 = Get(vec);
    for (R_xlen_t i = 0; i < n; i++) {
      switch(data1.encodings[i]) {
      case 1:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_NATIVE) );
        break;
      case 2:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_UTF8) );
        break;
      case 3:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_LATIN1) );
        break;
      case 4:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_BYTES) );
        break;
      default:
        SET_STRING_ELT(data2, i, NA_STRING);
      break;
      }
    }
    
    // free up some memory -- shrink to fit is a non-binding request
    data1.encodings.resize(0);
    data1.encodings.shrink_to_fit();
    data1.strings.resize(0);
    data1.strings.shrink_to_fit();
    
    R_set_altrep_data2(vec, data2);
    UNPROTECT(1);
    return std::move(data2);
  }
  
  // The start of the data, i.e. the underlying double* array from the std::vector<double>
  // This is guaranteed to never allocate (in the R sense)
  static const void* Dataptr_or_null(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 == R_NilValue) return nullptr;
    return STDVEC_DATAPTR(data2);
  }
  
  // same in this case, writeable is ignored
  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }
  
  
  // ALTSTRING methods -----------------
  // the element at the index `i`
  // this does not do bounds checking because that's expensive, so
  // the caller must take care of that
  static SEXP string_Elt(SEXP vec, R_xlen_t i){
    auto data2 = Materialize(vec);
    return STRING_ELT(data2, i);
    // switch(data1.encodings[i]) {
    // case 1:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_NATIVE);
    // case 2:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_UTF8);
    // case 3:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_LATIN1);
    // case 4:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_BYTES);
    // default:
    //   return NA_STRING;
    // break;
    // }
  }
  
  // -------- initialize the altrep class with the methods above
  
  static void Init(DllInfo* dll){
    class_t = R_make_altstring_class("stdvec_string", "altrepisode", dll);
    
    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);
    
    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    
    // altstring
    R_set_altstring_Elt_method(class_t, string_Elt);
  }
  
};

// static initialization of stdvec_double::class_t
R_altrep_class_t stdvec_string::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_stdvec_double(DllInfo* dll){
  stdvec_string::Init(dll);
}

////////////////////////////////////////////////////////////////
// common utility functions and constants
////////////////////////////////////////////////////////////////

bool is_big_endian();

// #define BLOCKSIZE 262144 // 2^20 bytes per block
#define BLOCKRESERVE 64
#define NA_STRING_LENGTH 4294967295 // 2^32-1 -- length used to signify NA value
#define MIN_SHUFFLE_ELEMENTS 4

static uint64_t BLOCKSIZE = 524288;

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

inline void writeSizeToFile8(std::ofstream & myFile, uint64_t x) {uint64_t x_temp = static_cast<uint64_t>(x); myFile.write(reinterpret_cast<char*>(&x_temp),8);}
inline void writeSizeToFile4(std::ofstream & myFile, uint64_t x) {uint32_t x_temp = static_cast<uint32_t>(x); myFile.write(reinterpret_cast<char*>(&x_temp),4);}
uint64_t readSizeFromFile4(std::ifstream & myFile) {
  std::array<char,4> a = {0,0,0,0};
  myFile.read(a.data(),4);
  return *reinterpret_cast<uint32_t*>(a.data());
}
uint64_t readSizeFromFile8(std::ifstream & myFile) {
  std::array<char,8> a = {0,0,0,0,0,0,0,0};
  myFile.read(a.data(),8);
  return *reinterpret_cast<uint64_t*>(a.data());
}

// unaligned cast to <POD>
template<typename POD>
inline POD unaligned_cast(char* data, uint64_t offset) {
  POD y;
  std::memcpy(&y, data + offset, sizeof(y));
  return y;
}


// qs reserve header details
// reserve[0] unused
// reserve[1] unused
// reserve[2] (low byte) shuffle control: 0x01 = logical shuffle, 0x02 = integer shuffle, 0x04 = double shuffle
// reserve[2] (high byte) algorithm: 0x10 = lz4, 0x00 = zstd
// reserve[3] endian: 1 = big endian, 0 = little endian
struct QsMetadata {
  unsigned char compress_algorithm;
  int compress_level;
  bool lgl_shuffle;
  bool int_shuffle;
  bool real_shuffle;
  bool cplx_shuffle;
  unsigned char endian;
  QsMetadata(std::string preset="balanced", std::string algorithm = "lz4", int compress_level=1, int shuffle_control=15) {
    if(preset == "fast") {
      this->compress_level = 150;
      shuffle_control = 0;
      compress_algorithm = 1;
    } else if(preset == "balanced") {
      this->compress_level = 1;
      shuffle_control = 15;
      compress_algorithm = 1;
    } else if(preset == "high") {
      this->compress_level = 4;
      shuffle_control = 15;
      compress_algorithm = 0;
    } else if(preset == "custom") {
      if(algorithm == "zstd") {
        compress_algorithm = 0;
        this->compress_level = compress_level;
        if(compress_level > 22 || compress_level < -50) throw exception("zstd compress_level must be an integer between -50 and 22");
      } else if(algorithm == "lz4") {
        compress_algorithm = 1;
        this->compress_level = compress_level;
        if(compress_level < 1) throw exception("lz4 compress_level must be an integer greater than 1");
      } else {
        throw exception("algorithm must be one of zstd or lz4");
      }
      if(shuffle_control < 0 || shuffle_control > 15) throw exception("shuffle_control must be an integer between 0 and 15");
    } else {
      throw exception("preset must be one of fast (default), balanced, high or custom");
    }
    lgl_shuffle = shuffle_control & 0x01;
    int_shuffle = shuffle_control & 0x02;
    real_shuffle = shuffle_control & 0x04;
    cplx_shuffle = shuffle_control & 0x08;
    endian = is_big_endian() ? 1 : 0;
  }
  QsMetadata(unsigned char shuffle_control, unsigned char algo, int cl) : 
    compress_algorithm(algo), compress_level(cl) {
    lgl_shuffle = shuffle_control & 0x01;
    int_shuffle = shuffle_control & 0x02;
    real_shuffle = shuffle_control & 0x04;
    cplx_shuffle = shuffle_control & 0x08;
    endian = is_big_endian() ? 1 : 0;
  }
  QsMetadata(std::ifstream & myFile) {
    std::array<unsigned char,4> reserve_bits;
    myFile.read(reinterpret_cast<char*>(reserve_bits.data()),4);
    unsigned char sys_endian = is_big_endian() ? 1 : 0;
    if(reserve_bits[3] != sys_endian) throw exception("Endian of system doesn't match file endian");
    compress_algorithm = reserve_bits[2] >> 4;
    lgl_shuffle = reserve_bits[2] & 0x01;
    int_shuffle = reserve_bits[2] & 0x02;
    real_shuffle = reserve_bits[2] & 0x04;
    cplx_shuffle = reserve_bits[2] & 0x08;
    endian = sys_endian;
  }
  void writeToFile(std::ofstream & myFile) {
    std::array<unsigned char,4> reserve_bits = {0,0,0,0};
    reserve_bits[2] += compress_algorithm << 4;
    reserve_bits[3] = is_big_endian() ? 1 : 0;
    reserve_bits[2] += (lgl_shuffle) + (int_shuffle << 1) + (real_shuffle << 2) + (cplx_shuffle << 3);
    myFile.write(reinterpret_cast<char*>(reserve_bits.data()),4); // some reserve bits for future use
  }
  void writeToFile(std::ofstream & myFile, unsigned char shuffle_control) {
    std::array<unsigned char,4> reserve_bits = {0,0,0,0};
    reserve_bits[2] += compress_algorithm << 4;
    reserve_bits[3] = is_big_endian() ? 1 : 0;
    reserve_bits[2] += shuffle_control;
    myFile.write(reinterpret_cast<char*>(reserve_bits.data()),4); // some reserve bits for future use
  }
};


// Normalize lz4/zstd function arguments so we can use function types
typedef size_t (*compress_fun)(void*, size_t, const void*, size_t, int);
typedef size_t (*decompress_fun)(void*, size_t, const void*, size_t);
typedef size_t (*cbound_fun)(size_t);

size_t LZ4_compressBound_fun(size_t srcSize) {
  return LZ4_compressBound(srcSize);
}

size_t LZ4_compress_fun( void* dst, size_t dstCapacity,
                         const void* src, size_t srcSize,
                                int compressionLevel) {
  return LZ4_compress_fast(reinterpret_cast<char*>(const_cast<void*>(src)), 
                           reinterpret_cast<char*>(const_cast<void*>(dst)),
                           static_cast<int>(srcSize), static_cast<int>(dstCapacity), compressionLevel);
}

size_t LZ4_decompress_fun( void* dst, size_t dstCapacity,
                           const void* src, size_t compressedSize) {
  return LZ4_decompress_safe(reinterpret_cast<char*>(const_cast<void*>(src)), 
                             reinterpret_cast<char*>(const_cast<void*>(dst)),
                             static_cast<int>(compressedSize), static_cast<int>(dstCapacity));
  
}

////////////////////////////////////////////////////////////////
// de-serialization functions
////////////////////////////////////////////////////////////////

struct Data_Context {
  std::ifstream & myFile;
  bool use_alt_rep_bool;
  
  QsMetadata qm;
  decompress_fun decompFun;
  cbound_fun cbFun;
  
  uint64_t number_of_blocks;
  std::vector<char> zblock;
  std::vector<char> block;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  uint64_t data_offset;
  uint64_t block_i;
  uint64_t block_size;
  std::string temp_string;

  Data_Context(std::ifstream & mf, QsMetadata qm, bool use_alt_rep) : myFile(mf), use_alt_rep_bool(use_alt_rep) {
    this->qm = qm;
    if(qm.compress_algorithm == 0) {
      decompFun = &ZSTD_decompress;
      cbFun = &ZSTD_compressBound;
    } else { // algo == 1
      decompFun = &LZ4_decompress_fun;
      cbFun = &LZ4_compressBound_fun;
    }
    number_of_blocks = readSizeFromFile8(myFile);
    zblock = std::vector<char>(cbFun(BLOCKSIZE));
    block = std::vector<char>(BLOCKSIZE);
    data_offset = 0;
    block_i = 0;
    block_size = 0;
    temp_string = std::string(256, '\0');
  }
  void readHeader(SEXPTYPE & object_type, uint64_t & r_array_len) {
    if(data_offset >= block_size) decompress_block();
    char* header = block.data();
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
      r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
      data_offset += 3;
      object_type = REALSXP;
      return;
    case numeric_header_32:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = REALSXP;
      return;
    case numeric_header_64:
      r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
      data_offset += 9;
      object_type = REALSXP;
      return;
    case list_header_8:
      r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
      data_offset += 2;
      object_type = VECSXP;
      return;
    case list_header_16:
      r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
      data_offset += 3;
      object_type = VECSXP;
      return;
    case list_header_32:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = VECSXP;
      return;
    case list_header_64:
      r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
      data_offset += 9;
      object_type = VECSXP;
      return;
    case integer_header_8:
      r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
      data_offset += 2;
      object_type = INTSXP;
      return;
    case integer_header_16:
      r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
      data_offset += 3;
      object_type = INTSXP;
      return;
    case integer_header_32:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = INTSXP;
      return;
    case integer_header_64:
      r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
      data_offset += 9;
      object_type = INTSXP;
      return;
    case logical_header_8:
      r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
      data_offset += 2;
      object_type = LGLSXP;
      return;
    case logical_header_16:
      r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
      data_offset += 3;
      object_type = LGLSXP;
      return;
    case logical_header_32:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = LGLSXP;
      return;
    case logical_header_64:
      r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
      data_offset += 9;
      object_type = LGLSXP;
      return;
    case raw_header_32:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = RAWSXP;
      return;
    case raw_header_64:
      r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
      data_offset += 9;
      object_type = RAWSXP;
      return;
    case character_header_8:
      r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
      data_offset += 2;
      object_type = STRSXP;
      return;
    case character_header_16:
      r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
      data_offset += 3;
      object_type = STRSXP;
      return;
    case character_header_32:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = STRSXP;
      return;
    case character_header_64:
      r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
      data_offset += 9;
      object_type = STRSXP;
      return;
    case complex_header_32:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = CPLXSXP;
      return;
    case complex_header_64:
      r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
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
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = ANYSXP;
      return;
    case nstype_header_32:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      object_type = S4SXP;
      return;
    case nstype_header_64:
      r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 9;
      object_type = S4SXP;
      return;
    }
    // additional types
    throw exception("something went wrong (reading object header)");
  }
  void readStringHeader(uint32_t & r_string_len, cetype_t & ce_enc) {
    if(data_offset >= block_size) decompress_block();
    char* header = block.data();
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
        r_string_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
        data_offset += 3;
        return;
      case string_header_32:
        r_string_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
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
  void decompress_direct(char* bpointer) {
    block_i++;
    std::array<char, 4> zsize_ar = {0,0,0,0};
    myFile.read(zsize_ar.data(), 4);
    uint64_t zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    myFile.read(zblock.data(), zsize);
    block_size = decompFun(bpointer, BLOCKSIZE, zblock.data(), zsize);
  }
  void decompress_block() {
    block_i++;
    std::array<char, 4> zsize_ar = {0,0,0,0};
    myFile.read(zsize_ar.data(), 4);
    uint64_t zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    myFile.read(zblock.data(), zsize);
    block_size = decompFun(block.data(), BLOCKSIZE, zblock.data(), zsize);
    data_offset = 0;
  }
  void getBlockData(char* outp, uint64_t data_size) {
    if(data_size <= block_size - data_offset) {
      memcpy(outp, block.data()+data_offset, data_size);
      data_offset += data_size;
    } else {
      uint64_t bytes_accounted = block_size - data_offset;
      memcpy(outp, block.data()+data_offset, bytes_accounted);
      while(bytes_accounted < data_size) {
        if(data_size - bytes_accounted >= BLOCKSIZE) {
          decompress_direct(outp+bytes_accounted);
          bytes_accounted += BLOCKSIZE;
        } else {
          decompress_block();
          memcpy(outp + bytes_accounted, block.data(), data_size - bytes_accounted);
          data_offset = data_size - bytes_accounted;
          bytes_accounted += data_offset;
        }
      }
    }
  }
  void getShuffleBlockData(char* outp, uint64_t data_size, uint64_t bytesoftype) {
    if(data_size >= MIN_SHUFFLE_ELEMENTS) {
      if(data_size > shuffleblock.size()) shuffleblock.resize(data_size);
      getBlockData(reinterpret_cast<char*>(shuffleblock.data()), data_size);
      blosc_unshuffle(shuffleblock.data(), reinterpret_cast<uint8_t*>(outp), data_size, bytesoftype);
    } else if(data_size > 0) {
      getBlockData(outp, data_size);
    }
  }
  SEXP processBlock() {
    SEXPTYPE obj_type;
    uint64_t r_array_len;
    readHeader(obj_type, r_array_len);
    uint64_t number_of_attributes;
    if(obj_type == ANYSXP) {
      number_of_attributes = r_array_len;
      readHeader(obj_type, r_array_len);
    } else {
      number_of_attributes = 0;
    }
    SEXP obj;
    switch(obj_type) {
    case VECSXP: 
      obj = PROTECT(Rf_allocVector(VECSXP, r_array_len));
      for(uint64_t i=0; i<r_array_len; i++) {
        SET_VECTOR_ELT(obj, i, processBlock());
      }
      break;
    case REALSXP:
      obj = PROTECT(Rf_allocVector(REALSXP, r_array_len));
      if(qm.real_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8);
      }
      break;
    case INTSXP:
      obj = PROTECT(Rf_allocVector(INTSXP, r_array_len));
      if(qm.int_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4);
      }
      break;
    case LGLSXP:
      obj = PROTECT(Rf_allocVector(LGLSXP, r_array_len));
      if(qm.lgl_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4);
      }
      break;
    case CPLXSXP:
      obj = PROTECT(Rf_allocVector(CPLXSXP, r_array_len));
      if(qm.cplx_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16);
      }
      break;
    case RAWSXP:
      obj = PROTECT(Rf_allocVector(RAWSXP, r_array_len));
      if(r_array_len > 0) getBlockData(reinterpret_cast<char*>(RAW(obj)), r_array_len);
      break;
    case STRSXP:
      if(use_alt_rep_bool) {
        auto ret = new stdvec_data(r_array_len);
        for(uint64_t i=0; i < r_array_len; i++) {
          uint32_t r_string_len;
          cetype_t string_encoding = CE_NATIVE;
          readStringHeader(r_string_len, string_encoding);
          if(r_string_len == NA_STRING_LENGTH) {
            ret->encodings[i] = 5;
          } else if(r_string_len == 0) {
            ret->encodings[i] = 1;
            ret->strings[i] = "";
          } else {
            temp_string.resize(r_string_len);
            getBlockData(&temp_string[0], r_string_len);
            switch(string_encoding) {
            case CE_NATIVE:
              ret->encodings[i] = 1;
              ret->strings[i] = temp_string;
              break;
            case CE_UTF8:
              ret->encodings[i] = 2;
              ret->strings[i] = temp_string;
              break;
            case CE_LATIN1:
              ret->encodings[i] = 3;
              ret->strings[i] = temp_string;
              break;
            case CE_BYTES:
              ret->encodings[i] = 4;
              ret->strings[i] = temp_string;
              break;
            default:
              ret->encodings[i] = 5;
            break;
            }
          }
        }
        obj = PROTECT(stdvec_string::Make(ret, true));
      } else {
        obj = PROTECT(Rf_allocVector(STRSXP, r_array_len));
        for(uint64_t i=0; i<r_array_len; i++) {
          uint32_t r_string_len;
          cetype_t string_encoding = CE_NATIVE;
          readStringHeader(r_string_len, string_encoding);
          if(r_string_len == NA_STRING_LENGTH) {
            SET_STRING_ELT(obj, i, NA_STRING);
          } else if(r_string_len == 0) {
            SET_STRING_ELT(obj, i, Rf_mkCharLen("", 0));
          } else if(r_string_len > 0) {
            if(r_string_len > temp_string.size()) {
              temp_string.resize(r_string_len);
            }
            getBlockData(&temp_string[0], r_string_len);
            SET_STRING_ELT(obj, i, Rf_mkCharLenCE(temp_string.data(), r_string_len, string_encoding));
          }
        }
      }
      break;
    case S4SXP:
    {
      SEXP obj_data = PROTECT(Rf_allocVector(RAWSXP, r_array_len));
      getBlockData(reinterpret_cast<char*>(RAW(obj_data)), r_array_len);
      obj = PROTECT(unserializeFromRaw(obj_data));
      UNPROTECT(2);
      return obj;
    }
    default: // also NILSXP
      obj = R_NilValue;
      return obj;
    }
    if(number_of_attributes > 0) {
      for(uint64_t i=0; i<number_of_attributes; i++) {
        uint32_t r_string_len;
        cetype_t string_encoding;
        readStringHeader(r_string_len, string_encoding);
        if(r_string_len > temp_string.size()) {
          temp_string.resize(r_string_len);
        }
        std::string attribute_name;
        attribute_name.resize(r_string_len);
        getBlockData(&attribute_name[0], r_string_len);
        Rf_setAttrib(obj, Rf_install(attribute_name.data()), processBlock());
      }
    }
    UNPROTECT(1);
    return std::move(obj);
  }
};


////////////////////////////////////////////////////////////////
// serialization functions
////////////////////////////////////////////////////////////////

struct CompressBuffer {
  uint64_t number_of_blocks = 0;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  std::vector<char> block = std::vector<char>(BLOCKSIZE);
  uint64_t current_blocksize=0;
  std::ofstream & myFile;
  QsMetadata qm;
  compress_fun compFun;
  cbound_fun cbFun;
  std::vector<char> zblock;

  CompressBuffer(std::ofstream & f, QsMetadata qm) : myFile(f) {
    this->qm = qm;
    if(qm.compress_algorithm == 0) {
      compFun = &ZSTD_compress;
      cbFun = &ZSTD_compressBound;
    } else { // algo == 1
      compFun = &LZ4_compress_fun;
      cbFun = &LZ4_compressBound_fun;
    }
    zblock = std::vector<char>(cbFun(BLOCKSIZE));
  }
  void flush() {
    if(current_blocksize > 0) {
      uint64_t zsize = compFun(zblock.data(), zblock.size(), block.data(), current_blocksize, qm.compress_level);
      writeSizeToFile4(myFile, zsize);
      myFile.write(zblock.data(), zsize);
      current_blocksize = 0;
      number_of_blocks++;
    }
  }
  void append(char* data, uint64_t len, bool contiguous = false) {
    uint64_t current_pointer_consumed = 0;
    while(current_pointer_consumed < len) {
      if( (current_blocksize == BLOCKSIZE) || ((BLOCKSIZE - current_blocksize < BLOCKRESERVE) && !contiguous) ) {
        flush();
      }
      if(current_blocksize == 0 && len - current_pointer_consumed >= BLOCKSIZE) {
        uint64_t zsize = compFun(zblock.data(), zblock.size(), data + current_pointer_consumed, BLOCKSIZE, qm.compress_level);
        writeSizeToFile4(myFile, zsize);
        myFile.write(zblock.data(), zsize);
        current_pointer_consumed += BLOCKSIZE;
        number_of_blocks++;
      } else {
        uint64_t remaining_pointer_available = len - current_pointer_consumed;
        uint64_t add_length = remaining_pointer_available < (BLOCKSIZE - current_blocksize) ? remaining_pointer_available : BLOCKSIZE-current_blocksize;
        memcpy(block.data() + current_blocksize, data + current_pointer_consumed, add_length);
        current_blocksize += add_length;
        current_pointer_consumed += add_length;
      }
    }
  }
  void shuffle_append(char* data, uint64_t len, size_t bytesoftype, bool contiguous = false) {
    if(len > MIN_SHUFFLE_ELEMENTS) {
      if(len > shuffleblock.size()) shuffleblock.resize(len);
      blosc_shuffle(reinterpret_cast<uint8_t*>(data), shuffleblock.data(), len, bytesoftype);
      append(reinterpret_cast<char*>(shuffleblock.data()), len, true);
    } else if(len > 0) {
      append(data, len, true);
    }
  }
  template<typename POD>
  inline void append_pod(POD pod, bool contiguous = false) {
    append(reinterpret_cast<char*>(&pod), sizeof(pod), contiguous);
  }
  
  void writeHeader(SEXPTYPE object_type, uint64_t length) {
    switch(object_type) {
    case REALSXP:
      if(length < 32) {
        append_pod(static_cast<unsigned char>( numeric_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_8)), 1);
        append_pod(static_cast<uint8_t>(length), true );
      } else if(length < 65536) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_16)), 1);
        append_pod(static_cast<uint16_t>(length), true );
      } else if(length < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_32)), 1);
        append_pod(static_cast<uint32_t>(length), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_64)), 1);
        append_pod(static_cast<uint64_t>(length), true );
      }
      return;
    case VECSXP:
      if(length < 32) {
        append_pod(static_cast<unsigned char>( list_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_8)), 1);
        append_pod(static_cast<uint8_t>(length), true );
      } else if(length < 65536) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_16)), 1);
        append_pod(static_cast<uint16_t>(length), true );
      } else if(length < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_32)), 1);
        append_pod(static_cast<uint32_t>(length), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_64)), 1);
        append_pod(static_cast<uint64_t>(length), true );
      }
      return;
    case INTSXP:
      if(length < 32) {
        append_pod(static_cast<unsigned char>( integer_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_8)), 1);
        append_pod(static_cast<uint8_t>(length), true );
      } else if(length < 65536) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_16)), 1);
        append_pod(static_cast<uint16_t>(length), true );
      } else if(length < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_32)), 1);
        append_pod(static_cast<uint32_t>(length), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_64)), 1);
        append_pod(static_cast<uint64_t>(length), true );
      }
      return;
    case LGLSXP:
      if(length < 32) {
        append_pod(static_cast<unsigned char>( logical_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_8)), 1);
        append_pod(static_cast<uint8_t>(length), true );
      } else if(length < 65536) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_16)), 1);
        append_pod(static_cast<uint16_t>(length), true );
      } else if(length < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_32)), 1);
        append_pod(static_cast<uint32_t>(length), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_64)), 1);
        append_pod(static_cast<uint64_t>(length), true );
      }
      return;
    case RAWSXP:
      if(length < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_32)), 1);
        append_pod(static_cast<uint32_t>(length), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_64)), 1);
        append_pod(static_cast<uint64_t>(length), true );
      }
      return;
    case STRSXP:
      if(length < 32) {
        append_pod(static_cast<unsigned char>( character_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_8)), 1);
        append_pod(static_cast<uint8_t>(length), true );
      } else if(length < 65536) { 
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_16)), 1);
        append_pod(static_cast<uint16_t>(length), true );
      } else if(length < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_32)), 1);
        append_pod(static_cast<uint32_t>(length), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_64)), 1);
        append_pod(static_cast<uint64_t>(length), true );
      }
      return;
    case CPLXSXP:
      if(length < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_32)), 1);
        append_pod(static_cast<uint32_t>(length), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_64)), 1);
        append_pod(static_cast<uint64_t>(length), true );
      }
      return;
    case NILSXP:
      append(reinterpret_cast<char*>(const_cast<unsigned char*>(&null_header)), 1);
      return;
    default:
      throw exception("something went wrong writing object header");  // should never reach here
    }
  }
  
  void writeAttributeHeader(uint64_t length) {
    if(length < 32) {
      append_pod(static_cast<unsigned char>( attribute_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) {
      append_pod(static_cast<unsigned char>( attribute_header_8 ) );
      append_pod(static_cast<uint8_t>(length), true );
    } else {
      append_pod(static_cast<unsigned char>( attribute_header_32 ) );
      append_pod(static_cast<uint32_t>(length), true );
    }
  }
  
  void writeStringHeader(uint64_t length, cetype_t ce_enc) {
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
      append_pod(static_cast<unsigned char>( string_header_5 | static_cast<unsigned char>(enc) | static_cast<unsigned char>(length) ) );
    } else if(length < 256) {
      append_pod(static_cast<unsigned char>( string_header_8 | static_cast<unsigned char>(enc) ) );
      append_pod(static_cast<uint8_t>(length), true );
    } else if(length < 65536) {
      append_pod(static_cast<unsigned char>( string_header_16 | static_cast<unsigned char>(enc) ) );
      append_pod(static_cast<uint16_t>(length), true );
    } else {
      append_pod(static_cast<unsigned char>( string_header_32 | static_cast<unsigned char>(enc) ) );
      append_pod(static_cast<uint32_t>(length), true );
    }
  }
  
  // to do: use SEXP instead of RObject?
  void appendObj(RObject & x, bool attributes_processed = false) {
    if(!attributes_processed && stypes.find(TYPEOF(x)) != stypes.end()) {
      std::vector<std::string> anames = x.attributeNames();
      if(anames.size() != 0) {
        writeAttributeHeader(anames.size());
        appendObj(x, true);
        for(uint64_t i=0; i<anames.size(); i++) {
          writeStringHeader(anames[i].size(),CE_NATIVE);
          append(&anames[i][0], anames[i].size(), true);
          RObject xa = x.attr(anames[i]);
          appendObj(xa);
        }
      } else {
        appendObj(x, true);
      }
    } else if(TYPEOF(x) == STRSXP) {
      uint64_t dl = Rf_xlength(x);
      writeHeader(STRSXP, dl);
      CharacterVector xc = CharacterVector(x);
      for(uint64_t i=0; i<dl; i++) {
        SEXP xi = xc[i];
        if(xi == NA_STRING) {
          append(reinterpret_cast<char*>(const_cast<unsigned char*>(&string_header_NA)), 1);
        } else {
          uint64_t dl = LENGTH(xi);
          writeStringHeader(dl, Rf_getCharCE(xi));
          append(const_cast<char*>(CHAR(xi)), dl, true);
        }
      }
    } else if(stypes.find(TYPEOF(x)) != stypes.end()) {
      uint64_t dl = Rf_xlength(x);
      writeHeader(TYPEOF(x), dl);
      if(TYPEOF(x) == VECSXP) {
        List xl = List(x);
        for(uint64_t i=0; i<dl; i++) {
          RObject xi = xl[i];
          appendObj(xi);
        }
      } else {
        switch(TYPEOF(x)) {
        case REALSXP:
          if(qm.real_shuffle) {
            shuffle_append(reinterpret_cast<char*>(REAL(x)), dl*8, 8, true);
          } else {
            append(reinterpret_cast<char*>(REAL(x)), dl*8, true); 
          }
          break;
        case INTSXP:
          if(qm.int_shuffle) {
            shuffle_append(reinterpret_cast<char*>(INTEGER(x)), dl*4, 4, true); break;
          } else {
            append(reinterpret_cast<char*>(INTEGER(x)), dl*4, true); 
          }
          break;
        case LGLSXP:
          if(qm.lgl_shuffle) {
            shuffle_append(reinterpret_cast<char*>(LOGICAL(x)), dl*4, 4, true); break;
          } else {
            append(reinterpret_cast<char*>(LOGICAL(x)), dl*4, true); 
          }
          break;
        case RAWSXP:
          append(reinterpret_cast<char*>(RAW(x)), dl, true); 
          break;
        case CPLXSXP:
          if(qm.cplx_shuffle) {
            shuffle_append(reinterpret_cast<char*>(COMPLEX(x)), dl*16, 8, true); break;
          } else {
            append(reinterpret_cast<char*>(COMPLEX(x)), dl*16, true); 
          }
          break;
        case NILSXP:
          break;
        }
      }
    } else { // other non-supported SEXPTYPEs use the built in R serialization method
      RawVector xserialized = serializeToRaw(x);
      if(xserialized.size() < 4294967296) {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&nstype_header_32)), 1);
        append_pod(static_cast<uint32_t>(xserialized.size()), true );
      } else {
        append(reinterpret_cast<char*>(const_cast<unsigned char*>(&nstype_header_64)), 1);
        append_pod(static_cast<uint64_t>(xserialized.size()), true );
      }
      append(reinterpret_cast<char*>(RAW(xserialized)), xserialized.size(), true);
    }
  }
};


