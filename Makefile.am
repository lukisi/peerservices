NULL = 
ACLOCAL_AMFLAGS = -I m4
.PHONY: check

noinst_LTLIBRARIES = \
	libpeers.la \
	$(NULL)

TESTS = \
	test_serialization \
	test_data_structs \
	$(NULL)

check_PROGRAMS = \
	test_serialization \
	test_data_structs \
	$(NULL)

# noinst_PROGRAMS =  \
# 	peersclient \
# 	$(NULL)

# TESTS = \
# 	test_serialization \
# 	test_1 \
# 	test_2 \
# 	test_3 \
# 	test_4 \
# 	test_5 \
# 	test_6 \
# 	$(NULL)

# check_PROGRAMS = \
# 	test_serialization \
# 	simulator \
# 	simulator2 \
# 	$(NULL)

libpeers_la_SOURCES = \
	peers.vala \
	serializables.vala \
	data_structs.vala \
	peers_debug.vala \
	serialize_helpers.vala \
	$(NULL)

libpeers_la_VALAFLAGS = \
	-H peers.h \
	--library peers \
	--vapidir $(GEE_PREFIX)/share/vala/vapi \
	--vapidir $(JSON_PREFIX)/share/vala-0.32/vapi \
	--vapidir $(TASKLETSYSTEM_VAPIDIR) \
	--vapidir $(NTKDCOMMON_VAPIDIR) \
	--vapidir $(NTKDRPC_VAPIDIR) \
	--pkg gee-0.8 \
	--pkg json-glib-1.0 \
	--pkg tasklet-system \
	--pkg ntkd-common \
	--pkg ntkdrpc\
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
	testsuites/test_serialization/serializables.vala \
	$(NULL)

test_serialization_VALAFLAGS = \
	--vapidir $(GEE_PREFIX)/share/vala/vapi \
	--vapidir $(JSON_PREFIX)/share/vala-0.32/vapi \
	--vapidir $(NTKDCOMMON_VAPIDIR) \
	--vapidir $(NTKDRPC_VAPIDIR) \
	--pkg gee-0.8 \
	--pkg json-glib-1.0 \
	--pkg ntkd-common \
	--pkg ntkdrpc \
	$(NULL)

test_serialization_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(JSONGLIB_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

test_serialization_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(JSONGLIB_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(NTKDRPC_LIBS) \
	$(NULL)

# test_data_structs does some tests on internal classes, hence we need to compile
#  it with source files, not with the convenience library. The source files
#  are symbolic links to avoid name clashes with vala-generated C files.
test_data_structs_SOURCES = \
	testsuites/data_structs/test_data_structs.vala \
	testsuites/data_structs/data_structs.vala \
	testsuites/data_structs/serializables.vala \
	$(NULL)

test_data_structs_VALAFLAGS = \
	--vapidir $(GEE_PREFIX)/share/vala/vapi \
	--vapidir $(JSON_PREFIX)/share/vala-0.32/vapi \
	--vapidir $(NTKDCOMMON_VAPIDIR) \
	--vapidir $(NTKDRPC_VAPIDIR) \
	--pkg gee-0.8 \
	--pkg json-glib-1.0 \
	--pkg ntkd-common \
	--pkg ntkdrpc \
	$(NULL)

test_data_structs_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(JSONGLIB_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

test_data_structs_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(JSONGLIB_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(NTKDRPC_LIBS) \
	$(NULL)

simulator_SOURCES =  \
	testsuites/simulator/simulator.vala \
	testsuites/simulator/tasklet_system_tasklet.vala \
	testsuites/simulator/service_ttl_100.vala \
	testsuites/simulator/simulator_debug.vala \
	peers.vapi \
	$(NULL)

simulator_VALAFLAGS = \
	--vapidir $(GEE_PREFIX)/share/vala/vapi \
	--vapidir $(JSON_PREFIX)/share/vala-0.32/vapi \
	--vapidir $(TASKLETSYSTEM_VAPIDIR) \
	--vapidir $(PTHTASKLET_VAPIDIR) \
	--vapidir $(NTKDCOMMON_VAPIDIR) \
	--vapidir $(NTKDRPC_VAPIDIR) \
	--pkg gee-0.8 \
	--pkg json-glib-1.0 \
	--pkg tasklet-system \
	--pkg pth-tasklet \
	--pkg ntkd-common \
	--pkg ntkdrpc\
	$(NULL)

simulator_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(JSONGLIB_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(PTHTASKLET_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

simulator_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(JSONGLIB_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(PTHTASKLET_LIBS) \
	$(NTKDRPC_LIBS) \
	$(builddir)/libpeers.la \
	$(NULL)

simulator2_SOURCES =  \
	testsuites/simulator2/simulator2.vala \
	testsuites/simulator2/tasklet_system_tasklet.vala \
	testsuites/simulator2/service_fk_101.vala \
	testsuites/simulator2/simulator2_debug.vala \
	peers.vapi \
	$(NULL)

simulator2_VALAFLAGS = \
	--vapidir $(GEE_PREFIX)/share/vala/vapi \
	--vapidir $(JSON_PREFIX)/share/vala-0.32/vapi \
	--vapidir $(TASKLETSYSTEM_VAPIDIR) \
	--vapidir $(PTHTASKLET_VAPIDIR) \
	--vapidir $(NTKDCOMMON_VAPIDIR) \
	--vapidir $(NTKDRPC_VAPIDIR) \
	--pkg gee-0.8 \
	--pkg ntkd-tasklet-system \
	--pkg tasklet \
	--pkg ntkdrpc \
	--pkg zcd-tasklet-system \
	--pkg json-glib-1.0 \
	$(NULL)

simulator2_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(JSONGLIB_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(TASKLET_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	$(NULL)

simulator2_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(JSONGLIB_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(TASKLET_LIBS) \
	$(NTKDRPC_LIBS) \
	$(builddir)/libpeers.la \
	$(NULL)

# peersclient_SOURCES =  \
# 	main.vala \
# 	tasklet_system_tasklet.vala \
# 	base_fake_address_manager.vala \
# 	peers.vapi \
# 	$(NULL)
# 
# peersclient_VALAFLAGS = \
# 	--pkg gee-0.8 \
# 	--pkg ntkd-common \
# 	--pkg ntkd-tasklet-system \
# 	--pkg tasklet \
# 	--pkg ntkdrpc \
# 	--pkg zcd-tasklet-system \
# 	$(NULL)
# 
# peersclient_CPPFLAGS = \
# 	-w \
# 	$(GOBJECT_CFLAGS) \
# 	$(GEE_CFLAGS) \
# 	$(NTKDCOMMON_CFLAGS) \
# 	$(TASKLETSYSTEM_CFLAGS) \
# 	$(TASKLET_CFLAGS) \
# 	$(NTKDRPC_CFLAGS) \
# 	$(NULL)
# 
# peersclient_LDADD = \
# 	$(GOBJECT_LIBS) \
# 	$(GEE_LIBS) \
# 	$(NTKDCOMMON_LIBS) \
# 	$(TASKLETSYSTEM_LIBS) \
# 	$(TASKLET_LIBS) \
# 	$(NTKDRPC_LIBS) \
# 	$(builddir)/libpeers.la \
# 	$(NULL)

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

