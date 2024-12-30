#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "Sorted_Set.hpp"
#include "usual.hpp"

static void ssnode_del(SSNode *node) {
    free(node);
}


static SSNode *ssnode_new(const char *name, size_t len, double score) {
    SSNode *node = (SSNode *)malloc(sizeof(SSNode) + len);
    assert(node);  
    avl_init(&node->tree);
    node->hmap.next = NULL;
    node->hmap.hashcode = str_hash((uint8_t *)name, len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0], name, len);
    return node;
}

static size_t min_node(size_t lhs, size_t rhs) {
    return lhs < rhs ? lhs : rhs;
}

static bool ssless(AVLNode *lhs, double score, const char *name, size_t len){
    SSNode *zl = container_of(lhs, SSNode, tree);
    if (zl->score != score) {
        return zl->score < score;
    }
    int rv = memcmp(zl->name, name, min_node(zl->len, len));
    if (rv != 0) {
        return rv < 0;
    }
    return zl->len < len;
}


static void sset_update(Sorted_Set *sset, SSNode *node, double score) {
    if (node->score == score) {
        return;
    }

}

struct HKey {
    HNode node;
    const char *name = NULL;
    size_t len = 0;
};

static bool hcmp(HNode *node, HNode *key) {
    SSNode *ssnode = container_of(node, SSNode, hmap);
    HKey *hkey = container_of(key, HKey, node);
    if (ssnode->len != hkey->len) {
        return false;
    }
    return 0 == memcmp(ssnode->name, hkey->name, ssnode->len);
}

SSNode *sset_lookup(Sorted_Set *sset, const char *name, size_t len) {
    if (!sset->root) {
        return NULL;
    }

    HKey key;
    key.node.hashcode = str_hash((uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hm_lookup(&sset->hmap, &key.node, &hcmp);
    return found ? container_of(found, SSNode, hmap) : NULL;
}


void sset_delete(Sorted_Set *sset, SSNode *node) {
    
    HKey key;
    key.node.hashcode = node->hmap.hashcode;
    key.name = node->name;
    key.len = node->len;
    HNode *found = hm_delete(&sset->hmap, &key.node, &hcmp);
    assert(found);
    
    sset->root = avl_del(&node->tree);
    ssnode_del(node);
}

SSNode *sset_seekge(Sorted_Set *sset, double score, const char *name, size_t len) {
    AVLNode *found = NULL;
    for (AVLNode *node = sset->root; node; ) {
        if (ssless(node, score, name, len)) {
            node = node->right; 
        } 
    return found ? container_of(found, SSNode, tree) : NULL;
}


SSNode *ssnode_offset(SSNode *node, int64_t offset) {
    AVLNode *tnode = node ? avl_off_set(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, SSNode, tree) : NULL;
}
