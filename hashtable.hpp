#pragma once

#include <stddef.h>
#include <stdint.h>

struct HNode {
    HNode *next = NULL;
    uint64_t hcode = 0;
};

struct HTab {
    HNode **tab = NULL; 
    size_t mask = 0;    
    size_t size = 0;    
};

struct HMap {
    HTab newer;
    HTab older;
    size_t migrate_pos = 0;
};
