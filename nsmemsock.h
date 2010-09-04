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
 * The Original Code is AOLserver Code and related documentation
 * distributed by AOL.
 *
 * The Initial Developer of the Original Code is America Online,
 * Inc. Portions created by AOL are Copyright (C) 1999 America Online,
 * Inc. All Rights Reserved.
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
 
/* Added by Majid: majidkhan59@yahoo.com
 * Following functions have been taken from Naviserver sock.c file
 * http://naviserver.cvs.sourceforge.net/naviserver/naviserver/nsd/sock.c?revision=1.17&view=markup
 * but have been modified according to AOL sock.c 
 */

size_t Ns_SetVec(struct iovec *iov, int i, CONST void *data, size_t len);

static int
SockSend(SOCKET sock, struct iovec *bufs, int nbufs)
{
#ifdef _WIN32
    int n;

    if (WSASend(sock, (LPWSABUF)bufs, nbufs, &n, 0, NULL, NULL) != 0) {
	n = -1;
    }
    return n;
#else
    struct msghdr msg;

    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = bufs;
    msg.msg_iovlen = nbufs;
    return sendmsg(sock, &msg, 0);
#endif
}

/*----------------------------------------------------------------------
 *
 * Ns_ResetVec --
 *
 *      Zero the bufs which have had their data sent and adjust
 *      the remainder.
 *
 * Results:
 *      Index of first buf to send.
 *
 * Side effects:
 *      Updates offset and length members.
 *
 *----------------------------------------------------------------------
*/

int
Ns_ResetVec(struct iovec *iov, int nbufs, size_t sent)
{
    int     i;
    void   *data;
    size_t  len;

    for (i = 0; i < nbufs && sent > 0; i++) {

        data = iov[i].iov_base;
        len  = iov[i].iov_len;

        if (len > 0) {
            if (sent >= len) {
                sent -= len;
                Ns_SetVec(iov, i, NULL, 0);
            } else {
                Ns_SetVec(iov, i, data + sent, len - sent);
                break;
            }
        }
    }
    return i;
}

/*----------------------------------------------------------------------
 *
 * Ns_SetVec --
 *
 *      Set the fields of the given iovec.
 *
 * Results:
 *      The given length.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
*/ 

size_t
Ns_SetVec(struct iovec *iov, int i, CONST void *data, size_t len)
{
    iov[i].iov_base = (void *) data;
    iov[i].iov_len = len;

    return len;
}


int Ns_SockSendBufs(SOCKET sock, struct iovec *bufs, int nbufs, int timeout, int flags)
{
    int           sbufLen, sbufIdx = 0, nsbufs = 0, bufIdx = 0;
    int           nwrote = 0, towrite = 0, sent = -1;
    void         *data;
    int           len;
    struct iovec  sbufs[UIO_MAXIOV], *sbufPtr;

    sbufPtr = sbufs;
    sbufLen = UIO_MAXIOV;

    while (bufIdx < nbufs || towrite > 0) {
	/*
         * Send up to UIO_MAXIOV buffers of data at a time and strip out
         * empty buffers.
         */
        while (bufIdx < nbufs && sbufIdx < sbufLen) {
	    data = bufs[bufIdx].iov_base;
            len  = bufs[bufIdx].iov_len;

            if (len > 0 && data != NULL) {
                towrite += Ns_SetVec(sbufPtr, sbufIdx++, data, len);
                nsbufs++;
            }
            bufIdx++;
        }
	/*
         * Timeout once if first attempt would block.
         */
	sent = SockSend(sock, sbufPtr, nsbufs);
        if (sent < 0
            && ns_sockerrno == EWOULDBLOCK
            && Ns_SockWait(sock, NS_SOCK_WRITE, timeout) == NS_OK) {
            sent = SockSend(sock, sbufPtr, nsbufs);
        }
        if (sent < 0) {
            break;
        }

        towrite -= sent;
        nwrote  += sent;

        if (towrite > 0) {
	    sbufIdx = Ns_ResetVec(sbufPtr, nsbufs, sent);
            nsbufs -= sbufIdx;

	     /*
             * If there are more whole buffers to send, move the remaining unsent
             * buffers to the beginning of the iovec array so that we always send
             * the maximum number of buffers the OS can handle.
             */

            if (bufIdx < nbufs - 1) {
                memmove(sbufPtr, sbufPtr + sbufIdx, (size_t) sizeof(struct iovec) * nsbufs);
            } else {
                sbufPtr = sbufPtr + sbufIdx;
                sbufLen = nsbufs - sbufIdx;
            }
        } else {
            nsbufs = 0;
        }
        sbufIdx = 0;
    }
    return nwrote ? nwrote : sent;
}

