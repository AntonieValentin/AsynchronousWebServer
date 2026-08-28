// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;


static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	snprintf(conn->send_buffer, BUFSIZ, "HTTP/1.1 200 OK\r\n"
				"Content-Length: %ld\r\n"
				"Connection: close\r\n"
				"\r\n", conn->file_size);
	conn->send_len = strlen(conn->send_buffer);
	conn->send_pos = 0;
}

static void connection_prepare_send_404(struct connection *conn)
{
	snprintf(conn->send_buffer, BUFSIZ, 
				"HTTP/1.1 404 Not Found\r\n"
				"Content-Length: 0\r\n"
				"Connection: close\r\n\r\n");
		conn->send_len = strlen(conn->send_buffer);
		conn->send_pos = 0;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	if (strstr(conn->request_path, "static"))
		return RESOURCE_TYPE_STATIC;
	else if (strstr(conn->request_path, "dynamic"))
		return RESOURCE_TYPE_DYNAMIC;
	else
		return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	struct connection *conn = malloc(sizeof(*conn));

	DIE(conn == NULL, "malloc");

	memset(conn, 0, sizeof(*conn));
	conn->fd = -1;

	conn->sockfd = sockfd;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
}

void connection_remove(struct connection *conn)
{
	close(conn->sockfd);
	close(conn->fd);
	conn->state = STATE_CONNECTION_CLOSED;
}

void handle_new_connection(void)
{
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");
	dlog(LOG_ERR, "Accepted connection from: %s:%d\n",
	inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

	int flags = fcntl(sockfd, F_GETFL, 0);
	dlog(flags == -1, "Error getting socket flags");

	rc = fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
	dlog(rc == -1, "Error setting socket non-blocking");

	conn = connection_create(sockfd);
	conn->state = STATE_INITIAL;

	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_in");

	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
}

void receive_data(struct connection *conn)
{
	ssize_t bytes_recv;
	int rc;
	char abuffer[BUFSIZ];

	rc = get_peer_address(conn->sockfd, abuffer, BUFSIZ);
	if (rc < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return;
	}

	bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ - conn->recv_len, 0);
	if (bytes_recv > 0)
		conn->recv_len += bytes_recv;
	if (bytes_recv < 0) {		/* error in communication */
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return;
		dlog(LOG_ERR, "Error in communication from: %s\n", abuffer);
		connection_remove(conn);
		return;
	}
	if (bytes_recv == 0) {		/* connection closed */
		struct epoll_event ev;
		ev.events = EPOLLOUT;
		ev.data.ptr = conn;
		epoll_ctl(epollfd, EPOLL_CTL_MOD, conn->sockfd, &ev);
		return;
	}

	dlog(LOG_DEBUG, "Received message from: %s\n", abuffer);

	printf("--\n%s--\n", conn->recv_buffer);

	conn->state = STATE_RECEIVING_DATA;
}

int connection_open_file(struct connection *conn)
{
	int fd = open(conn->request_path + 1, O_RDONLY);
	dlog(LOG_DEBUG, "%s\n", conn->request_path + 1);
	if (fd == -1) {
		dlog(LOG_DEBUG, "open");
    	return -1;
	}
	struct stat st;
	if (fstat(fd, &st) == -1) {
		dlog(LOG_DEBUG, "fstat");
		close(fd);
		return -1;
	}
	conn->fd = fd;
	strcpy(conn->filename, (strrchr(conn->request_path, '/') + 1));
	conn->file_size = st.st_size;
	return fd;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
}

int parse_header(struct connection *conn)
{
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};
	size_t bytes_parsed;
	dlog(LOG_DEBUG, "Parsing http\n");
	bytes_parsed = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);
    DIE(bytes_parsed != conn->recv_len, "HTTP parse error\n");

	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{

	ssize_t bytes = sendfile(conn->sockfd, conn->fd, &conn->file_pos, conn->file_size - conn->file_pos);
	if (bytes < 0)
		return conn->state;
	if (conn->file_pos == conn->file_size)
		conn->state = STATE_DATA_SENT;
	return STATE_DATA_SENT;
}

int connection_send_data(struct connection *conn)
{
	ssize_t bytes_sent;
	int rc;
	char abuffer[BUFSIZ];

	rc = get_peer_address(conn->sockfd, abuffer, BUFSIZ);
	if (rc < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return;
	}
	bytes_sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len - conn->send_pos, 0);

	if (bytes_sent > 0)
		conn->send_pos += bytes_sent;
	if (bytes_sent < 0) {		/* error in communication */
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return STATE_SENDING_DATA;
		dlog(LOG_ERR, "Error in communication to %s\n", abuffer);
		connection_remove(conn);
		return;
	}
	if (bytes_sent == 0) {		/* connection closed */
		dlog(LOG_INFO, "Connection closed to %s\n", abuffer);
		connection_remove(conn);
		return;
	}

	dlog(LOG_DEBUG, "Sending message to %s\n", abuffer);

	printf("--\n%s--\n", conn->send_buffer);

	return STATE_SENDING_HEADER;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */
	int rc;
	switch (conn->state) {
	default:
		if (conn->state == STATE_INITIAL || conn->state == STATE_RECEIVING_DATA) {
			receive_data(conn);

			/* add socket to epoll for out events */
			if (strstr(conn->recv_buffer, "\r\n\r\n") != NULL) {
					conn->state = STATE_SENDING_DATA;
					rc = w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
					DIE(rc < 0, "w_epoll_add_ptr_inout");
				}
			}
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */

	switch (conn->state) {
		default:
			int rc = conn->fd;
			if (conn->state == STATE_SENDING_DATA){
				parse_header(conn);
				rc = connection_open_file(conn);
				if (rc == -1){
					connection_prepare_send_404(conn);
					conn->state = STATE_SENDING_404;
				}
				else if (rc >= 0 && connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC){
					connection_prepare_send_reply_header(conn);
					conn->state = STATE_SENDING_HEADER;
				}
				else{
                    connection_prepare_send_404(conn);
                    conn->state = STATE_SENDING_404;
				}
			}
			if (conn->state == STATE_SENDING_HEADER || conn->state == STATE_SENDING_404) {
				connection_send_data(conn);

				if (conn->send_pos == conn->send_len) {
					if (conn->state == STATE_SENDING_404) {
						connection_remove(conn);
						return;
					}
					conn->state = STATE_HEADER_SENT;
				} 
				else  
					return;
			}
			if (rc >= 0 && connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC){
				connection_send_static(conn);
				if (conn->state == STATE_DATA_SENT) {
					connection_remove(conn);
				}
			}
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	if (event & EPOLLIN) {
		dlog(LOG_DEBUG, "New message\n");
		handle_input(conn);
	}
	if ((event & EPOLLOUT) && conn->state != STATE_CONNECTION_CLOSED) {
		dlog(LOG_DEBUG, "Ready to send message\n");
		handle_output(conn);
	}
	if (conn->state == STATE_CONNECTION_CLOSED)
		free(conn);
}

int main(void)
{
	int rc;

	/* initialize asynchronous operations. */

	rc = io_setup(10, &ctx);
	DIE(rc < 0, "io_setup");

	/* init multiplexing */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	/* create server socket */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT,
		DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	dlog(LOG_INFO, "Server waiting for connections on port %d\n",
		AWS_LISTEN_PORT);


	/* server main loop */
	while (1) {
		struct epoll_event rev;

		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");
		if (rev.data.fd == listenfd) {
			dlog(LOG_DEBUG, "New connection\n");
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			handle_client(rev.events, rev.data.ptr);
		}
	}


	return 0;
}
