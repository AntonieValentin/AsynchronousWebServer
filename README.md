# Asynchronous Web Server

A small HTTP web server implemented in C for Linux. The project focuses on handling multiple client connections efficiently using asynchronous and non-blocking I/O mechanisms.

The server supports a limited part of HTTP, mainly serving files to clients, while using an event-driven architecture based on `epoll`.

## Overview

Each client connection is represented by its own state machine. The server keeps track of the current state of every connection and advances it whenever the corresponding socket or file operation is ready.

HTTP requests are parsed to determine the requested resource. Depending on the path, the server serves either a static file or a dynamic file.

## Main Features

- non-blocking TCP sockets;
- multiple simultaneous connections using `epoll`;
- HTTP request parsing;
- asynchronous file operations;
- zero-copy file transfers with `sendfile`;
- support for static and dynamic files;
- proper handling of partial reads and writes;
- HTTP `200` and `404` responses;
- connection state management.

## Static and Dynamic Files

Static resources are served from the `static/` directory and are transferred using `sendfile`, avoiding an unnecessary copy of the file contents through user space.

Files from the `dynamic/` directory are read using the asynchronous file I/O interface. Their contents are then sent to the client through the non-blocking socket.

Requests for resources that do not exist result in an HTTP `404 Not Found` response.

## Event-Driven Architecture

The main event loop relies on `epoll` to monitor the listening socket and all active client connections. Instead of blocking while waiting for one client, the server can make progress on multiple connections as events become available.

This approach is especially useful when several clients request files at the same time.

## Project Structure

- `aws.c` - main server implementation;
- `awh.h` - connection and state definitions;
- `aws.h` - server configuration and constants;
- `http-parser/` - HTTP request parsing support;
- `static/` - static resources;
- `dynamic/` - dynamic resources.

## Notes

The main challenge of the project is coordinating different types of I/O while keeping the server responsive. Socket readiness, asynchronous file operations, partial transfers, and connection state all have to work together without blocking other clients.

The result is a compact event-driven web server that demonstrates several important Linux I/O mechanisms in a single project.
