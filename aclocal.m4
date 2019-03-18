# generated automatically by aclocal 1.15 -*- Autoconf -*-

# Copyright (C) 1996-2014 Free Software Foundation, Inc.

# This file is free software; the Free Software Foundation
# gives unlimited permission to copy and/or distribute it,
# with or without modifications, as long as this notice is preserved.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY, to the extent permitted by law; without
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE.

m4_ifndef([AC_CONFIG_MACRO_DIRS], [m4_defun([_AM_CONFIG_MACRO_DIRS], [])m4_defun([AC_CONFIG_MACRO_DIRS], [_AM_CONFIG_MACRO_DIRS($@)])])
# ===========================================================================
#   http://www.gnu.org/software/autoconf-archive/ax_check_compile_flag.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_CHECK_COMPILE_FLAG(FLAG, [ACTION-SUCCESS], [ACTION-FAILURE], [EXTRA-FLAGS], [INPUT])
#
# DESCRIPTION
#
#   Check whether the given FLAG works with the current language's compiler
#   or gives an error.  (Warnings, however, are ignored)
#
#   ACTION-SUCCESS/ACTION-FAILURE are shell commands to execute on
#   success/failure.
#
#   If EXTRA-FLAGS is defined, it is added to the current language's default
#   flags (e.g. CFLAGS) when the check is done.  The check is thus made with
#   the flags: "CFLAGS EXTRA-FLAGS FLAG".  This can for example be used to
#   force the compiler to issue an error when a bad flag is given.
#
#   INPUT gives an alternative input source to AC_COMPILE_IFELSE.
#
#   NOTE: Implementation based on AX_CFLAGS_GCC_OPTION. Please keep this
#   macro in sync with AX_CHECK_{PREPROC,LINK}_FLAG.
#
# LICENSE
#
#   Copyright (c) 2008 Guido U. Draheim <guidod@gmx.de>
#   Copyright (c) 2011 Maarten Bosmans <mkbosmans@gmail.com>
#
#   This program is free software: you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation, either version 3 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <http://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

#serial 4

AC_DEFUN([AX_CHECK_COMPILE_FLAG],
[AC_PREREQ(2.64)dnl for _AC_LANG_PREFIX and AS_VAR_IF
AS_VAR_PUSHDEF([CACHEVAR],[ax_cv_check_[]_AC_LANG_ABBREV[]flags_$4_$1])dnl
AC_CACHE_CHECK([whether _AC_LANG compiler accepts $1], CACHEVAR, [
  ax_check_save_flags=$[]_AC_LANG_PREFIX[]FLAGS
  _AC_LANG_PREFIX[]FLAGS="$[]_AC_LANG_PREFIX[]FLAGS $4 $1"
  AC_COMPILE_IFELSE([m4_default([$5],[AC_LANG_PROGRAM()])],
    [AS_VAR_SET(CACHEVAR,[yes])],
    [AS_VAR_SET(CACHEVAR,[no])])
  _AC_LANG_PREFIX[]FLAGS=$ax_check_save_flags])
AS_VAR_IF(CACHEVAR,yes,
  [m4_default([$2], :)],
  [m4_default([$3], :)])
AS_VAR_POPDEF([CACHEVAR])dnl
])dnl AX_CHECK_COMPILE_FLAGS

# ===========================================================================
#          http://www.gnu.org/software/autoconf-archive/ax_ext.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_EXT
#
# DESCRIPTION
#
#   Find supported SIMD extensions by requesting cpuid. When a SIMD
#   extension is found, the -m"simdextensionname" is added to SIMD_FLAGS if
#   compiler supports it. For example, if "sse2" is available then "-msse2"
#   is added to SIMD_FLAGS.
#
#   Find other supported CPU extensions by requesting cpuid. When a
#   processor extension is found, the -m"extensionname" is added to
#   CPUEXT_FLAGS if compiler supports it. For example, if "bmi2" is
#   available then "-mbmi2" is added to CPUEXT_FLAGS.
#
#   This macro calls:
#
#     AC_SUBST(SIMD_FLAGS)
#     AC_SUBST(CPUEXT_FLAGS)
#
#   And defines:
#
#     HAVE_RDRND / HAVE_BMI1 / HAVE_BMI2 / HAVE_ADX / HAVE_MPX
#     HAVE_PREFETCHWT1 / HAVE_ABM / HAVE_MMX / HAVE_SSE / HAVE_SSE2
#     HAVE_SSE3 / HAVE_SSSE3 / HAVE_SSE4_1 / HAVE_SSE4_2 / HAVE_SSE4a
#     HAVE_SHA / HAVE_AES / HAVE_AVX / HAVE_FMA3 / HAVE_FMA4 / HAVE_XOP
#     HAVE_AVX2 / HAVE_AVX512_F / HAVE_AVX512_CD / HAVE_AVX512_PF
#     HAVE_AVX512_ER / HAVE_AVX512_VL / HAVE_AVX512_BW / HAVE_AVX512_DQ
#     HAVE_AVX512_IFMA / HAVE_AVX512_VBMI
#
# LICENSE
#
#   Copyright (c) 2007 Christophe Tournayre <turn3r@users.sourceforge.net>
#   Copyright (c) 2013,2015 Michael Petch <mpetch@capp-sysware.com>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

#serial 15

AC_DEFUN([AX_EXT],
[
  AC_REQUIRE([AC_CANONICAL_HOST])
  AC_REQUIRE([AC_PROG_CC])

  CPUEXT_FLAGS=""
  SIMD_FLAGS=""

  case $host_cpu in
    powerpc*)
      AC_CACHE_CHECK([whether altivec is supported], [ax_cv_have_altivec_ext],
          [
            if test `/usr/sbin/sysctl -a 2>/dev/null| grep -c hw.optional.altivec` != 0; then
                if test `/usr/sbin/sysctl -n hw.optional.altivec` = 1; then
                  ax_cv_have_altivec_ext=yes
                fi
            fi
          ])

          if test "$ax_cv_have_altivec_ext" = yes; then
            AC_DEFINE(HAVE_ALTIVEC,,[Support Altivec instructions])
            AX_CHECK_COMPILE_FLAG(-faltivec, SIMD_FLAGS="$SIMD_FLAGS -faltivec", [])
          fi
    ;;

    i[[3456]]86*|x86_64*|amd64*)

      AC_REQUIRE([AX_GCC_X86_CPUID])
      AC_REQUIRE([AX_GCC_X86_CPUID_COUNT])
      AC_REQUIRE([AX_GCC_X86_AVX_XGETBV])

      eax_cpuid0=0
      AX_GCC_X86_CPUID(0x00000000)
      if test "$ax_cv_gcc_x86_cpuid_0x00000000" != "unknown";
      then
        eax_cpuid0=`echo $ax_cv_gcc_x86_cpuid_0x00000000 | cut -d ":" -f 1`
      fi

      eax_cpuid80000000=0
      AX_GCC_X86_CPUID(0x80000000)
      if test "$ax_cv_gcc_x86_cpuid_0x80000000" != "unknown";
      then
        eax_cpuid80000000=`echo $ax_cv_gcc_x86_cpuid_0x80000000 | cut -d ":" -f 1`
      fi

      ecx_cpuid1=0
      edx_cpuid1=0
      if test "$((0x$eax_cpuid0))" -ge 1 ; then
        AX_GCC_X86_CPUID(0x00000001)
        if test "$ax_cv_gcc_x86_cpuid_0x00000001" != "unknown";
        then
          ecx_cpuid1=`echo $ax_cv_gcc_x86_cpuid_0x00000001 | cut -d ":" -f 3`
          edx_cpuid1=`echo $ax_cv_gcc_x86_cpuid_0x00000001 | cut -d ":" -f 4`
        fi
      fi

      ebx_cpuid7=0
      ecx_cpuid7=0
      if test "$((0x$eax_cpuid0))" -ge 7 ; then
        AX_GCC_X86_CPUID_COUNT(0x00000007, 0x00)
        if test "$ax_cv_gcc_x86_cpuid_0x00000007" != "unknown";
        then
          ebx_cpuid7=`echo $ax_cv_gcc_x86_cpuid_0x00000007 | cut -d ":" -f 2`
          ecx_cpuid7=`echo $ax_cv_gcc_x86_cpuid_0x00000007 | cut -d ":" -f 3`
        fi
      fi

      ecx_cpuid80000001=0
      edx_cpuid80000001=0
      if test "$((0x$eax_cpuid80000000))" -ge "$((0x80000001))" ; then
        AX_GCC_X86_CPUID(0x80000001)
        if test "$ax_cv_gcc_x86_cpuid_0x80000001" != "unknown";
        then
          ecx_cpuid80000001=`echo $ax_cv_gcc_x86_cpuid_0x80000001 | cut -d ":" -f 3`
          edx_cpuid80000001=`echo $ax_cv_gcc_x86_cpuid_0x80000001 | cut -d ":" -f 4`
        fi
      fi

      AC_CACHE_VAL([ax_cv_have_mmx_os_support_ext],
      [
        ax_cv_have_mmx_os_support_ext=yes
      ])

      ax_cv_have_none_os_support_ext=yes

      AC_CACHE_VAL([ax_cv_have_sse_os_support_ext],
      [
        ax_cv_have_sse_os_support_ext=no,
        if test "$((0x$edx_cpuid1>>25&0x01))" = 1; then
          AC_LANG_PUSH([C])
          AC_TRY_RUN([
#include <signal.h>
#include <stdlib.h>
            /* No way at ring1 to ring3 in protected mode to check the CR0 and CR4
               control registers directly. Execute an SSE instruction.
               If it raises SIGILL then OS doesn't support SSE based instructions */
            void sig_handler(int signum){ exit(1); }
            int main(){
              signal(SIGILL, sig_handler);
              /* SSE instruction xorps  %xmm0,%xmm0 */
              __asm__ __volatile__ (".byte 0x0f, 0x57, 0xc0");
              return 0;
            }],
            ax_cv_have_sse_os_support_ext=yes,
            ax_cv_have_sse_os_support_ext=no,
            ax_cv_have_sse_os_support_ext=no)
          AC_LANG_POP([C])
        fi
      ])

      xgetbv_eax=0
      if test "$((0x$ecx_cpuid1>>28&0x01))" = 1; then
        AX_GCC_X86_AVX_XGETBV(0x00000000)

        if test x"$ax_cv_gcc_x86_avx_xgetbv_0x00000000" != x"unknown"; then
          xgetbv_eax=`echo $ax_cv_gcc_x86_avx_xgetbv_0x00000000 | cut -d ":" -f 1`
        fi

        AC_CACHE_VAL([ax_cv_have_avx_os_support_ext],
        [
          ax_cv_have_avx_os_support_ext=no
          if test "$((0x$ecx_cpuid1>>27&0x01))" = 1; then
            if test "$((0x$xgetbv_eax&0x6))" = 6; then
              ax_cv_have_avx_os_support_ext=yes
            fi
          fi
        ])
      fi

      AC_CACHE_VAL([ax_cv_have_avx512_os_support_ext],
      [
        ax_cv_have_avx512_os_support_ext=no
        if test "$ax_cv_have_avx_os_support_ext" = yes; then
          if test "$((0x$xgetbv_eax&0xe6))" = "$((0xe6))"; then
            ax_cv_have_avx512_os_support_ext=yes
          fi
        fi
      ])

      for ac_instr_info dnl
      in "none;rdrnd;RDRND;ecx_cpuid1,30;-mrdrnd;HAVE_RDRND;CPUEXT_FLAGS" dnl
         "none;bmi1;BMI1;ebx_cpuid7,3;-mbmi;HAVE_BMI1;CPUEXT_FLAGS" dnl
         "none;bmi2;BMI2;ebx_cpuid7,8;-mbmi2;HAVE_BMI2;CPUEXT_FLAGS" dnl
         "none;adx;ADX;ebx_cpuid7,19;-madx;HAVE_ADX;CPUEXT_FLAGS" dnl
         "none;mpx;MPX;ebx_cpuid7,14;-mmpx;HAVE_MPX;CPUEXT_FLAGS" dnl
         "none;prefetchwt1;PREFETCHWT1;ecx_cpuid7,0;-mprefetchwt1;HAVE_PREFETCHWT1;CPUEXT_FLAGS" dnl
         "none;abm;ABM;ecx_cpuid80000001,5;-mabm;HAVE_ABM;CPUEXT_FLAGS" dnl
         "mmx;mmx;MMX;edx_cpuid1,23;-mmmx;HAVE_MMX;SIMD_FLAGS" dnl
         "sse;sse;SSE;edx_cpuid1,25;-msse;HAVE_SSE;SIMD_FLAGS" dnl
         "sse;sse2;SSE2;edx_cpuid1,26;-msse2;HAVE_SSE2;SIMD_FLAGS" dnl
         "sse;sse3;SSE3;ecx_cpuid1,1;-msse3;HAVE_SSE3;SIMD_FLAGS" dnl
         "sse;ssse3;SSSE3;ecx_cpuid1,9;-mssse3;HAVE_SSSE3;SIMD_FLAGS" dnl
         "sse;sse41;SSE4.1;ecx_cpuid1,19;-msse4.1;HAVE_SSE4_1;SIMD_FLAGS" dnl
         "sse;sse42;SSE4.2;ecx_cpuid1,20;-msse4.2;HAVE_SSE4_2;SIMD_FLAGS" dnl
         "sse;sse4a;SSE4a;ecx_cpuid80000001,6;-msse4a;HAVE_SSE4a;SIMD_FLAGS" dnl
         "sse;sha;SHA;ebx_cpuid7,29;-msha;HAVE_SHA;SIMD_FLAGS" dnl
         "sse;aes;AES;ecx_cpuid1,25;-maes;HAVE_AES;SIMD_FLAGS" dnl
         "avx;avx;AVX;ecx_cpuid1,28;-mavx;HAVE_AVX;SIMD_FLAGS" dnl
         "avx;fma3;FMA3;ecx_cpuid1,12;-mfma;HAVE_FMA3;SIMD_FLAGS" dnl
         "avx;fma4;FMA4;ecx_cpuid80000001,16;-mfma4;HAVE_FMA4;SIMD_FLAGS" dnl
         "avx;xop;XOP;ecx_cpuid80000001,11;-mxop;HAVE_XOP;SIMD_FLAGS" dnl
         "avx;avx2;AVX2;ebx_cpuid7,5;-mavx2;HAVE_AVX2;SIMD_FLAGS" dnl
         "avx512;avx512f;AVX512-F;ebx_cpuid7,16;-mavx512f;HAVE_AVX512_F;SIMD_FLAGS" dnl
         "avx512;avx512cd;AVX512-CD;ebx_cpuid7,28;-mavx512cd;HAVE_AVX512_CD;SIMD_FLAGS" dnl
         "avx512;avx512pf;AVX512-PF;ebx_cpuid7,26;-mavx512pf;HAVE_AVX512_PF;SIMD_FLAGS" dnl
         "avx512;avx512er;AVX512-ER;ebx_cpuid7,27;-mavx512er;HAVE_AVX512_ER;SIMD_FLAGS" dnl
         "avx512;avx512vl;AVX512-VL;ebx_cpuid7,31;-mavx512vl;HAVE_AVX512_VL;SIMD_FLAGS" dnl
         "avx512;avx512bw;AVX512-BW;ebx_cpuid7,30;-mavx512bw;HAVE_AVX512_BW;SIMD_FLAGS" dnl
         "avx512;avx512dq;AVX512-DQ;ebx_cpuid7,17;-mavx512dq;HAVE_AVX512_DQ;SIMD_FLAGS" dnl
         "avx512;avx512ifma;AVX512-IFMA;ebx_cpuid7,21;-mavx512ifma;HAVE_AVX512_IFMA;SIMD_FLAGS" dnl
         "avx512;avx512vbmi;AVX512-VBMI;ecx_cpuid7,1;-mavx512vbmi;HAVE_AVX512_VBMI;SIMD_FLAGS" dnl
         #
      do ac_instr_os_support=$(eval echo \$ax_cv_have_$(echo $ac_instr_info | cut -d ";" -f 1)_os_support_ext)
         ac_instr_acvar=$(echo $ac_instr_info | cut -d ";" -f 2)
         ac_instr_shortname=$(echo $ac_instr_info | cut -d ";" -f 3)
         ac_instr_chk_loc=$(echo $ac_instr_info | cut -d ";" -f 4)
         ac_instr_chk_reg=0x$(eval echo \$$(echo $ac_instr_chk_loc | cut -d "," -f 1))
         ac_instr_chk_bit=$(echo $ac_instr_chk_loc | cut -d "," -f 2)
         ac_instr_compiler_flags=$(echo $ac_instr_info | cut -d ";" -f 5)
         ac_instr_have_define=$(echo $ac_instr_info | cut -d ";" -f 6)
         ac_instr_flag_type=$(echo $ac_instr_info | cut -d ";" -f 7)

         AC_CACHE_CHECK([whether ${ac_instr_shortname} is supported by the processor], [ax_cv_have_${ac_instr_acvar}_cpu_ext],
         [
           eval ax_cv_have_${ac_instr_acvar}_cpu_ext=no
           if test "$((${ac_instr_chk_reg}>>${ac_instr_chk_bit}&0x01))" = 1 ; then
             eval ax_cv_have_${ac_instr_acvar}_cpu_ext=yes
           fi
         ])

         if test x"$(eval echo \$ax_cv_have_${ac_instr_acvar}_cpu_ext)" = x"yes"; then
           AC_CACHE_CHECK([whether ${ac_instr_shortname} is supported by the processor and OS], [ax_cv_have_${ac_instr_acvar}_ext],
           [
             eval ax_cv_have_${ac_instr_acvar}_ext=no
             if test x"${ac_instr_os_support}" = x"yes"; then
               eval ax_cv_have_${ac_instr_acvar}_ext=yes
             fi
           ])

           if test "$(eval echo \$ax_cv_have_${ac_instr_acvar}_ext)" = yes; then
             AX_CHECK_COMPILE_FLAG(${ac_instr_compiler_flags}, eval ax_cv_support_${ac_instr_acvar}_ext=yes,
                                                               eval ax_cv_support_${ac_instr_acvar}_ext=no)
             if test x"$(eval echo \$ax_cv_support_${ac_instr_acvar}_ext)" = x"yes"; then
               eval ${ac_instr_flag_type}=\"\$${ac_instr_flag_type} ${ac_instr_compiler_flags}\"
               AC_DEFINE_UNQUOTED([${ac_instr_have_define}])
             else
               AC_MSG_WARN([Your processor and OS supports ${ac_instr_shortname} instructions but not your compiler, can you try another compiler?])
             fi
           else
             if test x"${ac_instr_os_support}" = x"no"; then
               AC_CACHE_VAL(ax_cv_support_${ac_instr_acvar}_ext, eval ax_cv_support_${ac_instr_acvar}_ext=no)
               AC_MSG_WARN([Your processor supports ${ac_instr_shortname}, but your OS doesn't])
             fi
           fi
         else
           AC_CACHE_VAL(ax_cv_have_${ac_instr_acvar}_ext, eval ax_cv_have_${ac_instr_acvar}_ext=no)
           AC_CACHE_VAL(ax_cv_support_${ac_instr_acvar}_ext, eval ax_cv_support_${ac_instr_acvar}_ext=no)
         fi
      done
  ;;
  esac

  AH_TEMPLATE([HAVE_RDRND],[Define to 1 to support Digital Random Number Generator])
  AH_TEMPLATE([HAVE_BMI1],[Define to 1 to support Bit Manipulation Instruction Set 1])
  AH_TEMPLATE([HAVE_BMI2],[Define to 1 to support Bit Manipulation Instruction Set 2])
  AH_TEMPLATE([HAVE_ADX],[Define to 1 to support Multi-Precision Add-Carry Instruction Extensions])
  AH_TEMPLATE([HAVE_MPX],[Define to 1 to support Memory Protection Extensions])
  AH_TEMPLATE([HAVE_PREFETCHWT1],[Define to 1 to support Prefetch Vector Data Into Caches WT1])
  AH_TEMPLATE([HAVE_ABM],[Define to 1 to support Advanced Bit Manipulation])
  AH_TEMPLATE([HAVE_MMX],[Define to 1 to support Multimedia Extensions])
  AH_TEMPLATE([HAVE_SSE],[Define to 1 to support Streaming SIMD Extensions])
  AH_TEMPLATE([HAVE_SSE2],[Define to 1 to support Streaming SIMD Extensions])
  AH_TEMPLATE([HAVE_SSE3],[Define to 1 to support Streaming SIMD Extensions 3])
  AH_TEMPLATE([HAVE_SSSE3],[Define to 1 to support Supplemental Streaming SIMD Extensions 3])
  AH_TEMPLATE([HAVE_SSE4_1],[Define to 1 to support Streaming SIMD Extensions 4.1])
  AH_TEMPLATE([HAVE_SSE4_2],[Define to 1 to support Streaming SIMD Extensions 4.2])
  AH_TEMPLATE([HAVE_SSE4a],[Define to 1 to support AMD Streaming SIMD Extensions 4a])
  AH_TEMPLATE([HAVE_SHA],[Define to 1 to support Secure Hash Algorithm Extension])
  AH_TEMPLATE([HAVE_AES],[Define to 1 to support Advanced Encryption Standard New Instruction Set (AES-NI)])
  AH_TEMPLATE([HAVE_AVX],[Define to 1 to support Advanced Vector Extensions])
  AH_TEMPLATE([HAVE_FMA3],[Define to 1 to support  Fused Multiply-Add Extensions 3])
  AH_TEMPLATE([HAVE_FMA4],[Define to 1 to support Fused Multiply-Add Extensions 4])
  AH_TEMPLATE([HAVE_XOP],[Define to 1 to support eXtended Operations Extensions])
  AH_TEMPLATE([HAVE_AVX2],[Define to 1 to support Advanced Vector Extensions 2])
  AH_TEMPLATE([HAVE_AVX512_F],[Define to 1 to support AVX-512 Foundation Extensions])
  AH_TEMPLATE([HAVE_AVX512_CD],[Define to 1 to support AVX-512 Conflict Detection Instructions])
  AH_TEMPLATE([HAVE_AVX512_PF],[Define to 1 to support AVX-512 Conflict Prefetch Instructions])
  AH_TEMPLATE([HAVE_AVX512_ER],[Define to 1 to support AVX-512 Exponential & Reciprocal Instructions])
  AH_TEMPLATE([HAVE_AVX512_VL],[Define to 1 to support AVX-512 Vector Length Extensions])
  AH_TEMPLATE([HAVE_AVX512_BW],[Define to 1 to support AVX-512 Byte and Word Instructions])
  AH_TEMPLATE([HAVE_AVX512_DQ],[Define to 1 to support AVX-512 Doubleword and Quadword Instructions])
  AH_TEMPLATE([HAVE_AVX512_IFMA],[Define to 1 to support AVX-512 Integer Fused Multiply Add Instructions])
  AH_TEMPLATE([HAVE_AVX512_VBMI],[Define to 1 to support AVX-512 Vector Byte Manipulation Instructions])
  AC_SUBST(SIMD_FLAGS)
  AC_SUBST(CPUEXT_FLAGS)
])

# ===========================================================================
#   http://www.gnu.org/software/autoconf-archive/ax_gcc_x86_avx_xgetbv.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_GCC_X86_AVX_XGETBV
#
# DESCRIPTION
#
#   On later x86 processors with AVX SIMD support, with gcc or a compiler
#   that has a compatible syntax for inline assembly instructions, run a
#   small program that executes the xgetbv instruction with input OP. This
#   can be used to detect if the OS supports AVX instruction usage.
#
#   On output, the values of the eax and edx registers are stored as
#   hexadecimal strings as "eax:edx" in the cache variable
#   ax_cv_gcc_x86_avx_xgetbv.
#
#   If the xgetbv instruction fails (because you are running a
#   cross-compiler, or because you are not using gcc, or because you are on
#   a processor that doesn't have this instruction),
#   ax_cv_gcc_x86_avx_xgetbv_OP is set to the string "unknown".
#
#   This macro mainly exists to be used in AX_EXT.
#
# LICENSE
#
#   Copyright (c) 2013 Michael Petch <mpetch@capp-sysware.com>
#
#   This program is free software: you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation, either version 3 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <http://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

#serial 2

AC_DEFUN([AX_GCC_X86_AVX_XGETBV],
[AC_REQUIRE([AC_PROG_CC])
AC_LANG_PUSH([C])
AC_CACHE_CHECK(for x86-AVX xgetbv $1 output, ax_cv_gcc_x86_avx_xgetbv_$1,
 [AC_RUN_IFELSE([AC_LANG_PROGRAM([#include <stdio.h>], [
     int op = $1, eax, edx;
     FILE *f;
      /* Opcodes for xgetbv */
      __asm__ __volatile__ (".byte 0x0f, 0x01, 0xd0"
        : "=a" (eax), "=d" (edx)
        : "c" (op));
     f = fopen("conftest_xgetbv", "w"); if (!f) return 1;
     fprintf(f, "%x:%x\n", eax, edx);
     fclose(f);
     return 0;
])],
     [ax_cv_gcc_x86_avx_xgetbv_$1=`cat conftest_xgetbv`; rm -f conftest_xgetbv],
     [ax_cv_gcc_x86_avx_xgetbv_$1=unknown; rm -f conftest_xgetbv],
     [ax_cv_gcc_x86_avx_xgetbv_$1=unknown])])
AC_LANG_POP([C])
])

# ===========================================================================
#     http://www.gnu.org/software/autoconf-archive/ax_gcc_x86_cpuid.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_GCC_X86_CPUID(OP)
#   AX_GCC_X86_CPUID_COUNT(OP, COUNT)
#
# DESCRIPTION
#
#   On Pentium and later x86 processors, with gcc or a compiler that has a
#   compatible syntax for inline assembly instructions, run a small program
#   that executes the cpuid instruction with input OP. This can be used to
#   detect the CPU type. AX_GCC_X86_CPUID_COUNT takes an additional COUNT
#   parameter that gets passed into register ECX before calling cpuid.
#
#   On output, the values of the eax, ebx, ecx, and edx registers are stored
#   as hexadecimal strings as "eax:ebx:ecx:edx" in the cache variable
#   ax_cv_gcc_x86_cpuid_OP.
#
#   If the cpuid instruction fails (because you are running a
#   cross-compiler, or because you are not using gcc, or because you are on
#   a processor that doesn't have this instruction), ax_cv_gcc_x86_cpuid_OP
#   is set to the string "unknown".
#
#   This macro mainly exists to be used in AX_GCC_ARCHFLAG.
#
# LICENSE
#
#   Copyright (c) 2008 Steven G. Johnson <stevenj@alum.mit.edu>
#   Copyright (c) 2008 Matteo Frigo
#   Copyright (c) 2015 Michael Petch <mpetch@capp-sysware.com>
#
#   This program is free software: you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation, either version 3 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <http://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

#serial 8

AC_DEFUN([AX_GCC_X86_CPUID],
[AX_GCC_X86_CPUID_COUNT($1, 0)
])

AC_DEFUN([AX_GCC_X86_CPUID_COUNT],
[AC_REQUIRE([AC_PROG_CC])
AC_LANG_PUSH([C])
AC_CACHE_CHECK(for x86 cpuid $1 output, ax_cv_gcc_x86_cpuid_$1,
 [AC_RUN_IFELSE([AC_LANG_PROGRAM([#include <stdio.h>], [
     int op = $1, level = $2, eax, ebx, ecx, edx;
     FILE *f;
      __asm__ __volatile__ ("cpuid"
        : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
        : "a" (op), "2" (level));

     f = fopen("conftest_cpuid", "w"); if (!f) return 1;
     fprintf(f, "%x:%x:%x:%x\n", eax, ebx, ecx, edx);
     fclose(f);
     return 0;
])],
     [ax_cv_gcc_x86_cpuid_$1=`cat conftest_cpuid`; rm -f conftest_cpuid],
     [ax_cv_gcc_x86_cpuid_$1=unknown; rm -f conftest_cpuid],
     [ax_cv_gcc_x86_cpuid_$1=unknown])])
AC_LANG_POP([C])
])

