NULL = 
ACLOCAL_AMFLAGS = -I m4
.PHONY: check

lib_LTLIBRARIES = \
	libpeers.la \
	$(NULL)

noinst_PROGRAMS =  \
	peersclient \
	$(NULL)

TESTS = \
	test_serialization \
	simulator \
	$(NULL)

check_PROGRAMS = \
	test_serialization \
	simulator \
	$(NULL)

libpeers_la_SOURCES = \
	peers.vala \
	serialize_helpers.vala \
	$(NULL)

libpeers_la_VALAFLAGS = \
	-H peers.h \
	--library peers \
	--pkg gee-0.8 \
	--pkg json-glib-1.0 \
	--pkg ntkd-common \
	--pkg ntkd-tasklet-system \
	--pkg ntkdrpc\
	--pkg zcd-tasklet-system \
	$(NULL)

libpeers_la_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(JSONGLIB_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

libpeers_la_LIBADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(JSONGLIB_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(NTKDRPC_LIBS) \
	$(NULL)

# test_serialization does some tests on internal classes, hence we need to compile
#  it with source files, not with the convenience library. The source files
#  are symbolic links to avoid name clashes with vala-generated C files.
test_serialization_SOURCES = \
	testsuites/test_serialization/test_serialization.vala \
	testsuites/test_serialization/peers.vala \
	testsuites/test_serialization/serialize_helpers.vala \
	$(NULL)

test_serialization_VALAFLAGS = \
	--pkg gee-0.8 \
	--pkg json-glib-1.0 \
	--pkg ntkd-common \
	--pkg ntkd-tasklet-system \
	--pkg ntkdrpc\
	--pkg zcd-tasklet-system \
	$(NULL)

test_serialization_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(JSONGLIB_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(TASKLET_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

test_serialization_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(JSONGLIB_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(TASKLET_LIBS) \
	$(NTKDRPC_LIBS) \
	$(NULL)

simulator_SOURCES =  \
	testsuites/simulator/simulator.vala \
	testsuites/simulator/tasklet_system_tasklet.vala \
	testsuites/simulator/service_ttl_100.vala \
	peers.vapi \
	$(NULL)

simulator_VALAFLAGS = \
	--pkg gee-0.8 \
	--pkg ntkd-tasklet-system \
	--pkg tasklet \
	--pkg ntkdrpc \
	--pkg zcd-tasklet-system \
	--pkg json-glib-1.0 \
	$(NULL)

simulator_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(JSONGLIB_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(TASKLET_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

simulator_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(JSONGLIB_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(TASKLET_LIBS) \
	$(NTKDRPC_LIBS) \
	$(builddir)/libpeers.la \
	$(NULL)

peersclient_SOURCES =  \
	main.vala \
	tasklet_system_tasklet.vala \
	base_fake_address_manager.vala \
	peers.vapi \
	$(NULL)

peersclient_VALAFLAGS = \
	--pkg gee-0.8 \
	--pkg ntkd-common \
	--pkg ntkd-tasklet-system \
	--pkg tasklet \
	--pkg ntkdrpc \
	--pkg zcd-tasklet-system \
	$(NULL)

peersclient_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(TASKLET_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

peersclient_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(TASKLET_LIBS) \
	$(NTKDRPC_LIBS) \
	$(builddir)/libpeers.la \
	$(NULL)

vapidir = $(datadir)/vala/vapi
dist_vapi_DATA = \
	peers.vapi \
	peers.deps \
	$(NULL)

include_HEADERS = \
	peers.h \
	$(NULL)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = \
	peers.pc \
	$(NULL)

EXTRA_DIST = \
	peers.pc.in \
	$(NULL)

MAINTAINERCLEANFILES = \
	$(srcdir)/peers.vapi \
	$(NULL)

peers.vapi: libpeers_la_vala.stamp
	$(NULL)

