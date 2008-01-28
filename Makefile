ifndef AOLSERVER
    AOLSERVER  = /bayt/software/aol/aolmajid-khan
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
