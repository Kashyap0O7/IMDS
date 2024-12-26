#pragma once
#include <stddef.h>
#include <stdint.h>


struct HNode {
    HNode *next = NULL;
    uint64_t hashcode = 0;
};

struct HTab {
    HNode **slots = NULL; 
    size_t mask = 0;    
    size_t size = 0;    
};

struct HMap {
    HTab bigger;
    HTab smaller;
    size_t mig_ptr = 0;
};

HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_insert(HMap *hmap, HNode *node);
HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_clear(HMap *hmap);
size_t hm_size(HMap *hmap);
void hm_foreach(HMap *hmap, bool (*fptr)(HNode *, void *), void *arg);
