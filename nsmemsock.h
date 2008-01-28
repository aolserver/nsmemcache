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
 
/* Added by Majid: majidkhan59@gmail.com
 * Following functions have been taken from Naviserver sock.c file
 * http://naviserver.cvs.sourceforge.net/naviserver/naviserver/nsd/sock.c?revision=1.17&view=markup
 * but have been modified according to AOL sock.c 
 */

/*
 *----------------------------------------------------------------------
 *
 * SockSend --
 *
 *      Send a vector of buffers on a non-blocking socket. Not all
 *      data may be sent.
 *
 * Results:
 *      Number of bytes sent or -1 on error.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
*/ 

static int
SockSend(SOCKET sock, struct iovec *bufs, int nbufs)
{
    int n;

#ifdef _WIN32
    if (WSASend(sock, (LPWSABUF)bufs, nbufs, &n, 0,
                NULL, NULL) != 0) {
        n = -1;
    }

    return n;
#else
    struct msghdr msg;

    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = bufs;
    msg.msg_iovlen = nbufs;

    n = sendmsg(sock, &msg, 0);

    if (n < 0) {
        Ns_Log(Debug, "SockSend: %s",
               ns_sockstrerror(ns_sockerrno));
    }
    return n;
#endif
}

/*
 *----------------------------------------------------------------------
 *
 * Ns_SockSendBufs --
 *
 *      Send a vector of buffers on a non-blocking socket. Not all
 *      data may be sent.
 *
 * Results:
 *      Number of bytes sent or -1 on error.
 *
 * Side effects:
 *      May wait for given timeout if first attempt would block.
 *
 *----------------------------------------------------------------------
 */

int
Ns_SockSendBufs(SOCKET sock, struct iovec *bufs, int nbufs,
                int timeout)
{
    int n;

    n = SockSend(sock, bufs, nbufs);
    if (n < 0
        && ns_sockerrno == EWOULDBLOCK
        && Ns_SockWait(sock, NS_SOCK_WRITE, timeout) == NS_OK) {
        n = SockSend(sock, bufs, nbufs);
    }

    return n;
}

/*----------------------------------------------------------------------
 *
 * Ns_SockWrite --
 *
 *      Timed write to a non-blocking socket.
 *      This function will try write all of the data
 *
 * Results:
 *      Number of bytes written, -1 for error
 *
 * Side effects:
 *      May wait given timeout.
 *
 *----------------------------------------------------------------------
 */

int
Ns_SockWrite(SOCKET sock, void *vbuf, size_t towrite, int time)
{
    int nwrote, n;
    char *buf;

    nwrote = towrite;
    buf = vbuf;
    while (towrite > 0) {
        n = Ns_SockSend(sock, buf, towrite, time);
        if (n < 0) {
            return -1;
        }
        towrite -= n;
        buf += n;
    }
    return nwrote;
}

/*
 *----------------------------------------------------------------------
 *
 * Ns_SockWriteV --
 *
 *      Send buffers to client, it will try to send all data
 *
 * Results:
 *      Number of bytes of given buffers written, -1 for
 *      error on first send.
 *
 * Side effects:
 *      May send data in multiple packets if nbufs is large.
 *
 *----------------------------------------------------------------------
 */

int
Ns_SockWriteV(SOCKET sock, struct iovec *bufs, int nbufs, int time)
{
    int           sbufLen, sbufIdx = 0, nsbufs = 0, bufIdx = 0;
    int           nwrote = 0, towrite = 0, sent = -1;
    struct iovec  sbufs[UIO_MAXIOV], *sbufPtr;

    sbufPtr = sbufs;
    sbufLen = UIO_MAXIOV;

    while (bufIdx < nbufs || towrite > 0) {

        while (bufIdx < nbufs && sbufIdx < sbufLen) {
            if (bufs[bufIdx].iov_len > 0 && bufs[bufIdx].iov_base != NULL) {
                sbufPtr[sbufIdx].iov_base = bufs[bufIdx].iov_base;
                sbufPtr[sbufIdx].iov_len = bufs[bufIdx].iov_len;
                towrite += sbufPtr[sbufIdx].iov_len;
                sbufIdx++;
                nsbufs++;
            }
            bufIdx++;
        }

        sent = Ns_SockSendBufs(sock, sbufPtr, nsbufs, time);
        if (sent < 0) {
            return -1;
        }
        towrite -= sent;
        nwrote  += sent;

        if (towrite > 0) {
            for (sbufIdx = 0; sbufIdx < nsbufs && sent > 0; sbufIdx++) {
                if (sent >= (int) sbufPtr[sbufIdx].iov_len) {
                    sent -= sbufPtr[sbufIdx].iov_len;
                } else {
                    sbufPtr[sbufIdx].iov_base = (char *) sbufPtr[sbufIdx].iov_base + sent;
                    sbufPtr[sbufIdx].iov_len -= sent;
                    break;
                }
            }
            if (bufIdx < nbufs - 1) {
                memmove(sbufPtr, sbufPtr + sbufIdx, (size_t) sizeof(struct iovec) * nsbufs);
            } else {
                sbufPtr = sbufPtr + sbufIdx;
                sbufLen = nsbufs - sbufIdx;
            }
            nsbufs -= sbufIdx;
        } else {
            nsbufs = 0;
        }
        sbufIdx = 0;
    }
    return nwrote;
}

