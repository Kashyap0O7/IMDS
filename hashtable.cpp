#include <stdlib.h>   
#include <assert.h>
#include "hashtable.hpp"

static void h_init(HTab *htab, size_t n) {
    assert(n > 0 && ((n - 1) & n) == 0);
    htab->slots = (HNode **)calloc(n, sizeof(HNode *));
    htab->mask = n - 1;
    htab->size = 0;
}


static void h_insert(HTab *htab, HNode *node) {
    size_t pos = node->hashcode & htab->mask;
    HNode *next = htab->slots[pos];
    node->next = next;
    htab->slots[pos] = node;
    htab->size++;
}

static HNode **h_lookup(HTab *htab, HNode *key, bool (*eq)(HNode *, HNode *)) {
    if (!htab->slots) {
        return NULL;
    }

    size_t pos = key->hashcode & htab->mask;
    HNode **from = &htab->slots[pos];    
    for (HNode *cur; (cur = *from) != NULL; from = &cur->next) {
        if (cur->hashcode == key->hashcode && eq(cur, key)) {
            return from;                
        }
    }
    return NULL;
}


static HNode *h_detach(HTab *htab, HNode **from) {
    HNode *node = *from;    
    *from = node->next;     
    htab->size--;
    return node;
}

const size_t k_rehashing_work = 256;    

static void hm_help_rehashing(HMap *hmap) {
    size_t nwork = 0;
    while (nwork < k_rehashing_work && hmap->smaller.size > 0) {
        
        HNode **from = &hmap->smaller.slots[hmap->mig_ptr];
        if (!*from) {
            hmap->mig_ptr++;
            continue;   
        }
        
        h_insert(&hmap->bigger, h_detach(&hmap->smaller, from));
        nwork++;
    }
    
    if (hmap->smaller.size == 0 && hmap->smaller.slots) {
        free(hmap->smaller.slots);
        hmap->smaller = HTab{};
    }
}

static void hm_trigger_rehashing(HMap *hmap) {
    assert(hmap->smaller.slots == NULL);
    
    hmap->smaller = hmap->bigger;
    h_init(&hmap->bigger, (hmap->bigger.mask + 1) * 2);
    hmap->mig_ptr = 0;
}

HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *)) {
    hm_help_rehashing(hmap);
    HNode **from = h_lookup(&hmap->bigger, key, eq);
    if (!from) {
        from = h_lookup(&hmap->smaller, key, eq);
    }
    return from ? *from : NULL;
}


static bool h_foreach(HTab *htab, bool (*fptr)(HNode *, void *), void *arg) {
    for (size_t i = 0; htab->mask != 0 && i <= htab->mask; i++) {
        for (HNode *node = htab->slots[i]; node != NULL; node = node->next) {
            if (!fptr(node, arg)) {
                return false;
            }
        }
    }
    return true;
}

const size_t k_max_load_factor = 16;

void hm_insert(HMap *hmap, HNode *node) {
    if (!hmap->bigger.slots) {
        h_init(&hmap->bigger, 4);    
    }
    h_insert(&hmap->bigger, node);   

    if (!hmap->smaller.slots) {         
        size_t shreshold = (hmap->bigger.mask + 1) * k_max_load_factor;
        if (hmap->bigger.size >= shreshold) {
            hm_trigger_rehashing(hmap);
        }
    }
    hm_help_rehashing(hmap);        
}

HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *)) {
    hm_help_rehashing(hmap);
    if (HNode **from = h_lookup(&hmap->bigger, key, eq)) {
        return h_detach(&hmap->bigger, from);
    }
    if (HNode **from = h_lookup(&hmap->smaller, key, eq)) {
        return h_detach(&hmap->smaller, from);
    }
    return NULL;
}

void hm_clear(HMap *hmap) {
    free(hmap->bigger.slots);
    free(hmap->smaller.slots);
    *hmap = HMap{};
}

size_t hm_size(HMap *hmap) {
    return hmap->bigger.size + hmap->smaller.size;
}

void hm_foreach(HMap *hmap, bool (*fptr)(HNode *, void *), void *arg) {
    h_foreach(&hmap->bigger, fptr, arg) && h_foreach(&hmap->smaller, fptr, arg);
}
