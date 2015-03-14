NULL = 
ACLOCAL_AMFLAGS = -I m4

EXTRA_LTLIBRARIES = \
	libpeers.la \
	$(NULL)

bin_PROGRAMS =  \
	peersclient \
	$(NULL)

TESTS = \
	test_peers \
	$(NULL)

check_PROGRAMS = \
	test_peers \
	$(NULL)

# test_peers does some tests on internal classes, hence we need to compile
#  it with source files, not with the convenience library. The source files
#  are symbolic links to avoid name clashes with vala-generated C files.
test_peers_SOURCES = \
	testsuites/test_peers/test_peers.vala \
	testsuites/test_peers/base_dispatcher.vala \
	testsuites/test_peers/peers.vala \
	$(NULL)

test_peers_VALAFLAGS = \
	--pkg gio-2.0 \
	--pkg gee-0.8 \
	--pkg tasklet \
	--pkg zcd \
	--pkg ntkd-rpc\
	$(NULL)

test_peers_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GIO_CFLAGS) \
	$(GEE_CFLAGS) \
	$(TASKLET_CFLAGS) \
	$(ZCD_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

test_peers_LDADD = \
	$(GOBJECT_LIBS) \
	$(GIO_LIBS) \
	$(GEE_LIBS) \
	$(TASKLET_LIBS) \
	$(ZCD_LIBS) \
	$(NTKDRPC_LIBS) \
	$(NULL)

peersclient_SOURCES =  \
	main.vala \
	peers.vapi \
	$(NULL)

peersclient_VALAFLAGS = \
	--pkg gio-2.0 \
	--pkg gee-0.8 \
	--pkg tasklet \
	--pkg posix \
	--pkg zcd \
	--pkg ntkd-rpc\
	--pkg linux \
	$(NULL)

peersclient_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GIO_CFLAGS) \
	$(GEE_CFLAGS) \
	$(TASKLET_CFLAGS) \
	$(ZCD_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

peersclient_LDADD = \
	$(GOBJECT_LIBS) \
	$(GIO_LIBS) \
	$(GEE_LIBS) \
	$(TASKLET_LIBS) \
	$(ZCD_LIBS) \
	$(NTKDRPC_LIBS) \
	$(builddir)/libpeers.la \
	$(NULL)

libpeers_la_SOURCES = \
	peers.vala \
	$(NULL)

libpeers_la_VALAFLAGS = \
	-H peers.h \
	--library peers \
	--pkg gio-2.0 \
	--pkg gee-0.8 \
	--pkg tasklet \
	--pkg posix \
	--pkg zcd \
	--pkg ntkd-rpc\
	--pkg linux \
	$(NULL)

libpeers_la_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GIO_CFLAGS) \
	$(GEE_CFLAGS) \
	$(TASKLET_CFLAGS) \
	$(ZCD_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

libpeers_la_LIBADD = -lm \
	$(GOBJECT_LIBS) \
	$(GIO_LIBS) \
	$(GEE_LIBS) \
	$(TASKLET_LIBS) \
	$(ZCD_LIBS) \
	$(NTKDRPC_LIBS) \
	$(NULL)

CLEANFILES = \
	$(srcdir)/peers.vapi \
	$(srcdir)/peers.h \
	$(srcdir)/libpeers_la_vala.stamp \
	$(srcdir)/main.c \
	$(srcdir)/peers.c \
	$(srcdir)/testsuites/test_peers/peers.c \
	$(srcdir)/testsuites/test_peers/test_peers.c \
	$(srcdir)/testsuites/test_peers/base_dispatcher.c \
	$(NULL)

peers.vapi: libpeers_la_vala.stamp
	$(NULL)

