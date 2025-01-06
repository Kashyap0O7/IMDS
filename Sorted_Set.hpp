#pragma once

#include "hashtable.hpp"
#include "AVLtree.hpp"


struct 
Sorted_Set {
    AVLNode *root = NULL;  
    HMap hmap;              
};

struct 
SSNode {
    AVLNode tree;
    HNode hmap;
    size_t len = 0;
    double score = 0;
    char name[0];       
};

SSNode *sset_lookup(Sorted_Set *sset, const char *name, size_t len);
bool sset_insert(Sorted_Set *sset, const char *name, size_t len, double score);
SSNode *sset_seekge(Sorted_Set *sset, double score, const char *name, size_t len);
void sset_delete(Sorted_Set *sset, SSNode *node);
void sset_clear(Sorted_Set *sset);
SSNode *ssnode_offset(SSNode *node, int64_t offset);
