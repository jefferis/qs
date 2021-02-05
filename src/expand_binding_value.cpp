/* qs - Quick Serialization of R Objects
 Copyright (C) 2019-present Travers Ching
 Copyright (C) 1998--2020  The R Core Team.
 Copyright (C) 1995, 1996  Robert Gentleman and Ross Ihaka
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.
 
 You can contact the author at:
 https://github.com/traversc/qs
 */


#define USE_RINTERNALS
#include <Rinternals.h>
#include <R.h>
#include "expand_binding_value.h"

#define CLEAR_BNDCELL_TAG(cell) do {		\
if (BNDCELL_TAG(cell)) {		            \
  CAR0(cell) = R_NilValue;		          \
  SET_BNDCELL_TAG(cell, 0);		         \
}					                                \
} while (0)

void SET_BNDCELL(SEXP cell, SEXP val)
{
  CLEAR_BNDCELL_TAG(cell);
  SETCAR(cell, val);
}

int get_bndcell_tag(SEXP e) {
  return BNDCELL_TAG(e);
}

void R_expand_binding_value(SEXP b) {
#if BOXED_BINDING_CELLS
  SET_BNDCELL_TAG(b, 0);
#else
  int typetag = BNDCELL_TAG(b);
  if (typetag) {
    union {
    SEXP sxpval;
    double dval;
    int ival;
  } vv;
    SEXP val;
    vv.sxpval = CAR0(b);
    switch (typetag) {
    case REALSXP:
      val = Rf_ScalarReal(vv.dval);
      SET_BNDCELL(b, val);
      INCREMENT_NAMED(val);
      break;
    case INTSXP:
      val = Rf_ScalarInteger(vv.ival);
      SET_BNDCELL(b, val);
      INCREMENT_NAMED(val);
      break;
    case LGLSXP:
      val = Rf_ScalarLogical(vv.ival);
      SET_BNDCELL(b, val);
      INCREMENT_NAMED(val);
      break;
    }
  }
#endif
}

SEXP R_make_binding_value(SEXP val) {
  SEXP x = PROTECT(Rf_cons(R_NilValue, R_NilValue));
  SET_TAG(x, Rf_install("boundval"));
  SEXPTYPE type = TYPEOF(val);
  INIT_BNDCELL(x, type);
  switch(type) {
  case INTSXP:
    SET_BNDCELL_IVAL(x, INTEGER(val)[0]);
    break;
  case REALSXP:
    SET_BNDCELL_DVAL(x, REAL(val)[0]);
    break;
  case LGLSXP:
    SET_BNDCELL_LVAL(x, LOGICAL(val)[0]);
    break;
  }
  UNPROTECT(1);
  return x;
}
