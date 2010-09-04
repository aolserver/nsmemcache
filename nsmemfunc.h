/*
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://mozilla.org/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * Alternatively, the contents of this file may be used under the terms
 * of the GNU General Public License (the "GPL"), in which case the
 * provisions of GPL are applicable instead of those above.  If you wish
 * to allow use of your version of this file only under the terms of the
 * GPL and not to allow others to use your version of this file under the
 * License, indicate your decision by deleting the provisions above and
 * replace them with the notice and other provisions required by the GPL.
 * If you do not delete the provisions above, a recipient may use your
 * version of this file under either the License or the GPL.
 */

/* Added By Majid majidkhan59@yahoo.com
 * Following structure and functions have been copied from tclobjv.c
 * http://naviserver.cvs.sourceforge.net/naviserver/naviserver/nsd/tclobjv.c?revision=1.17&view=markup
 */

/*
 * The following struct describes how to process an option
 * or argument passed to a Tcl command.
 */
struct Ns_ObjvSpec;
typedef int   (Ns_ObjvProc) (struct Ns_ObjvSpec *spec, Tcl_Interp *interp,
                             int *objcPtr, Tcl_Obj *CONST objv[]);
typedef struct Ns_ObjvTable {
    char            *key;
    int              value;
} Ns_ObjvTable;

typedef struct Ns_ObjvSpec {
    char            *key;
    Ns_ObjvProc     *proc;
    void            *dest;
    void            *arg;
} Ns_ObjvSpec;

/*
 *----------------------------------------------------------------------
 *
 * Ns_ObjvInt, Ns_ObjvLong, Ns_ObjvWideInt, Ns_ObjvDouble --
 *
 *      Consume exactly one argument, returning it's value into dest.
 *
 * Results:
 *      TCL_OK or TCL_ERROR;
 *
 * Side effects:
 *      Argument maybe converted to type specific internal rep.
 *
 *----------------------------------------------------------------------
 */

int Ns_ObjvInt(Ns_ObjvSpec *spec, Tcl_Interp *interp, int *objcPtr,Tcl_Obj *CONST objv[])
{
    int *dest = spec->dest;

    if (*objcPtr > 0 && Tcl_GetIntFromObj(interp, objv[0], dest) == TCL_OK) {
        *objcPtr -= 1;
        return TCL_OK;
    }
    return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * Ns_ObjvString --
 *
 *      Consume exactly one argument, returning a pointer to it's
 *      cstring into *spec->dest.
 *
 *      If spec->arg is != NULL it is assumed to be a pointer to an
 *      int and the returned string length will be left in it.
 *
 * Results:
 *      TCL_OK or TCL_ERROR.
 *
 * Side effects:
 *          None.
 *
 *----------------------------------------------------------------------
 */

int Ns_ObjvString(Ns_ObjvSpec *spec, Tcl_Interp *interp, int *objcPtr,Tcl_Obj *CONST objv[])
{
    char **dest = spec->dest;

    if (*objcPtr > 0) {
        *dest = Tcl_GetStringFromObj(objv[0], (int *) spec->arg);
        *objcPtr -= 1;
        return TCL_OK;
    }
    return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * Ns_ObjvBreak --
 *
 *          Handle '--' option/argument separator.
 *
 * Results:
 *      Always TCL_BREAK.
 *
 * Side effects:
 *      Option processing will end successfully, argument processing
 *      will begin.
 *
 *----------------------------------------------------------------------
 */

int Ns_ObjvBreak(Ns_ObjvSpec *spec, Tcl_Interp *interp,int *objcPtr, Tcl_Obj *CONST objv[])
{
    return TCL_BREAK;
}

/*
 *----------------------------------------------------------------------
 *
 * Ns_ObjvBool --
 *
 *      If spec->arg is 0 consume exactly one argument and attempt
 *      conversion to a boolean value.  Otherwise, spec->arg is treated
 *      as an int and placed into spec->dest with zero args consumed.
 *
 * Results:
 *      TCL_OK or TCL_ERROR.
 *
 * Side effects:
 *          Next Tcl object maybe converted to boolean type.
 *
 *----------------------------------------------------------------------
 */

int Ns_ObjvBool(Ns_ObjvSpec *spec, Tcl_Interp *interp, int *objcPtr,Tcl_Obj *CONST objv[])
{
    int *dest = spec->dest;

    if (spec->arg) {
        *dest = (int) spec->arg;
        return TCL_OK;
    }
    if (*objcPtr > 0 && Tcl_GetBooleanFromObj(interp, objv[0], dest) == TCL_OK) {
        *objcPtr -= 1;
        return TCL_OK;
    }
    return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * WrongNumArgs --
 *
 *          Leave a usage message in the interpreters result.
 *
 * Results:
 *          None.
 *
 * Side effects:
 *          None.
 *
 *----------------------------------------------------------------------
 */

static void WrongNumArgs(Ns_ObjvSpec *optSpec, Ns_ObjvSpec *argSpec, Tcl_Interp *interp,int objc, Tcl_Obj *CONST objv[])
{
    Ns_ObjvSpec *specPtr;
    Ns_DString   ds;
    char        *p;

    Ns_DStringInit(&ds);
    if (optSpec != NULL) {
        for (specPtr = optSpec; specPtr->key != NULL; ++specPtr) {
            if (STREQ(specPtr->key, "--")) {
                Ns_DStringAppend(&ds, "?--? ");
            } else if (specPtr->proc == &Ns_ObjvBool && specPtr->arg != NULL) {
                Ns_DStringPrintf(&ds, "?%s? ", specPtr->key);
            } else {
                p = specPtr->key;
                if (*specPtr->key == '-') {
                    ++p;
                }
                Ns_DStringPrintf(&ds, "?%s %s? ", specPtr->key, p);
            }
        }
    }
    if (argSpec != NULL) {
        for (specPtr = argSpec; specPtr->key != NULL; ++specPtr) {
            Ns_DStringPrintf(&ds, "%s%s ", specPtr->key,
                             (*specPtr->key == '?') ? "?" : "");
        }
    }
    Ns_DStringTrunc(&ds, Ns_DStringLength(&ds) - 1);
    Tcl_WrongNumArgs(interp, objc, objv, ds.string);
    Ns_DStringFree(&ds);
}


/*
 *----------------------------------------------------------------------
 *
 * Ns_ParseObjv --
 *
 *      Process objv acording to given option and arg specs.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Depends on the Ns_ObjvTypeProcs which run.
 *
 *----------------------------------------------------------------------
 */

int Ns_ParseObjv(Ns_ObjvSpec *optSpec, Ns_ObjvSpec *argSpec, Tcl_Interp *interp,int offset, int objc, Tcl_Obj *CONST objv[])
{
    Ns_ObjvSpec *specPtr = NULL;
    int          optIndex, status, remain = (objc - offset);

    if (optSpec && optSpec->key) {
        while (remain > 0) {
            if (Tcl_GetIndexFromObjStruct(NULL, objv[objc - remain], optSpec,
                                          sizeof(Ns_ObjvSpec), "option",
                                          TCL_EXACT, &optIndex) != TCL_OK) {
                break;
            }
            --remain;
            specPtr = optSpec + optIndex;
            status = specPtr->proc(specPtr, interp, &remain,
                                   objv + (objc - remain));
            if (status == TCL_BREAK) {
                break;
            } else if (status != TCL_OK) {
                return NS_ERROR;
            }
        }
    }
    if (argSpec == NULL) {
        if (remain > 0) {
        badargs:
            WrongNumArgs(optSpec, argSpec, interp, offset, objv);
            return NS_ERROR;
        }
        return NS_OK;
    }
    for (specPtr = argSpec; specPtr->key != NULL; specPtr++) {
        if (remain == 0) {
            if (specPtr->key[0] != '?') {
                goto badargs; /* Too few args. */
            }
            return NS_OK;
        }
        if (specPtr->proc(specPtr, interp, &remain, objv + (objc - remain))
            != TCL_OK) {
            return NS_ERROR;
        }
    }
    if (remain > 0) {
        goto badargs; /* Too many args. */
    }

    return NS_OK;
}

