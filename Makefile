ifndef AOLSERVER
    AOLSERVER  = ../aolserver/
endif

#
# Module name
#
MOD      =  nsmemcache.so

#
# Objects to build.
#
OBJS     = nsmemcache.o

include  $(AOLSERVER)/include/Makefile.module
