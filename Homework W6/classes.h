// Evgeny Ovechnikov
// Course: CS540 - Database Management Systems
// Homework: Hash Indexing (w5)

#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
#include <map>
using namespace std;

const int NUM_BLOCKS = 2;   // 2 blocks for storage buffer, 1 block for index buffer
const int BLOCK_SIZE = 4096 - 12;   // initialize the block size allowed in main memory according to the question. 12 bytes for overhead.
const int MAX_RECORDS = 65536;      // maximum projected number of records
const uint16_t REC_MEM_SIZE = 28;
const uint16_t REC_PHYS_SIZE = 28;
const uint16_t REC_NAME_ATTR_MAX_LEN = 200;
const uint16_t REC_BIO_ATTR_MAX_LEN = 500;

//const string INDEX_FILE_NAME = "EmployeeIndex";
const string DATA_FILE_NAME = "EmployeeRelation";
const int MAGIC_INT_EMPTY = 0x80000000;
const int MAX_CAPACITY_THRESHOLD = 70;
const int BUCKET_CAPACITY = 4;      // The number of records held by each bucket, or "Page" from lecture slides
const int BUCKET_PHYS_SIZE = (3 + 4 * BUCKET_CAPACITY) * 4;

// I'll refactor the global variables by next assignment
int globalBucketId = 0;
uint64_t globalLRU = 0;

// Class representing a tuple from the Employee.csv file
class Record {
public:
    int id, manager_id;
    std::string bio, name;

    Record(vector<std::string> fields) {
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }

    Record(uint64_t uid, string uname, string ubio, uint64_t managerId)
    {
        id = uid;
        name = uname;
        bio = ubio;
        manager_id = managerId;
    }

    // Default constructor to make compiler happy
    Record() { }

    int size() {
        return REC_MEM_SIZE + name.length() + bio.length();
    }

    void print() {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }
};

// Class representing memory block
class Block {
private:
    int nextFree;
    char data[BLOCK_SIZE];

public:
    bool dirty;
    uint64_t lru;

    Block() {
        clearBlock();
    }

    void clearBlock() {
        memset(data, 0, BLOCK_SIZE);
        nextFree = 0;
        lru = globalLRU;
        dirty = false;
    }

    bool insertRecord(Record* rec, int pos) {
        if (BLOCK_SIZE - nextFree < rec->size() ||
            rec->name.length() > REC_NAME_ATTR_MAX_LEN ||
            rec->bio.length() > REC_BIO_ATTR_MAX_LEN)
        {
            return false;
        }

        char* free = data;
        free += nextFree;
        int record_size = REC_MEM_SIZE; // fixed-length header

        // W5 addition: position
        *reinterpret_cast<int*>(free) = static_cast<int>(pos);
        free += sizeof(int);

        // Id
        *reinterpret_cast<uint64_t*>(free) = rec->id;
        free += sizeof(uint64_t);

        // Name Length
        *reinterpret_cast<uint32_t*>(free) = static_cast<uint32_t>(rec->name.length());
        free += sizeof(uint32_t);
        record_size += rec->name.length();

        // Bio Length
        *reinterpret_cast<uint32_t*>(free) = static_cast<uint32_t>(rec->bio.length());
        free += sizeof(uint32_t);
        record_size += rec->bio.length();

        // Manager id
        *reinterpret_cast<uint64_t*>(free) = rec->manager_id;
        free += sizeof(uint64_t);

        // Name
        for (int i = 0; i < rec->name.length(); i++) {
            *reinterpret_cast<char*>(free) = rec->name[i];
            free++;
        }

        // Bio
        for (int i = 0; i < rec->bio.length(); i++) {
            *reinterpret_cast<char*>(free) = rec->bio[i];
            free++;
        }

        nextFree += record_size;
        dirty = true;
        globalLRU++;
        lru = globalLRU;
        return true;
    }

    map<int, Record> getAllRecords() {
        map<int, Record> result;
        for (char* addr = data; addr < data + nextFree; )
        {
            int pos = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            uint64_t id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            uint32_t name_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint32_t bio_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint64_t manager_id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            string name(addr, name_len);
            addr += name_len;

            string bio(addr, bio_len);
            addr += bio_len;

            result[pos] = Record(id, name, bio, manager_id);
        }
        return result;
    }

    vector<uint64_t> getAllIds() {
        vector<uint64_t> result;
        for (char* addr = data; addr < data + nextFree; )
        {
            int pos = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            uint64_t id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            uint32_t name_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint32_t bio_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            addr += sizeof(uint64_t);
            addr += name_len;
            addr += bio_len;

            result.push_back(id);
        }
        return result;
    }

    Record getRecordById(uint64_t idToGet) {
        for (char* addr = data; addr < data + nextFree; )
        {
            int pos = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            uint64_t id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            uint32_t name_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint32_t bio_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint64_t manager_id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            string name(addr, name_len);
            addr += name_len;

            string bio(addr, bio_len);
            addr += bio_len;

            if (id == idToGet) {
                return Record(id, name, bio, manager_id);
            }
        }

        // The code should never reach this line. Don't call this method with non-existing Id.
        return Record();
    }
};

// Storage Buffer Manager class
class StorageBufferManager {
private:
    // You may declare variables based on your need 
    int lastFreeSpace = MAX_RECORDS * REC_PHYS_SIZE;
    Block* myBuffer;
    string myStorageName;
    fstream storageStream;

    // Save info from the buffer to disk
    void saveRecords(map<int, Record>* records) {
        storageStream.open(myStorageName, ios::binary | ios::in | ios::out);

        // iterating over map
        map<int, Record>::iterator it;
        for (it = records->begin(); it != records->end(); it++) {
            uint32_t nameAddr = lastFreeSpace;
            uint32_t bioAddr = lastFreeSpace + it->second.name.length();
            lastFreeSpace += it->second.name.length() + it->second.bio.length();
            // Casting stuff
            char recordCast[REC_PHYS_SIZE];
            memset(recordCast, 0, REC_PHYS_SIZE);
            char* t = recordCast;
            *reinterpret_cast<uint64_t*>(t) = it->second.id;
            t += sizeof(uint64_t);
            *reinterpret_cast<uint16_t*>(t) = it->second.name.length();
            t += sizeof(uint16_t);
            *reinterpret_cast<uint32_t*>(t) = nameAddr;
            t += sizeof(uint32_t);
            *reinterpret_cast<uint16_t*>(t) = it->second.bio.length();
            t += sizeof(uint16_t);
            *reinterpret_cast<uint32_t*>(t) = bioAddr;
            t += sizeof(uint32_t);
            *reinterpret_cast<uint64_t*>(t) = it->second.manager_id;

            streamoff offset = it->first * REC_PHYS_SIZE;
            storageStream.seekp(offset, ios_base::beg);
            storageStream.write(recordCast, REC_PHYS_SIZE);

            storageStream.seekp(nameAddr, ios_base::beg);
            storageStream.write(it->second.name.c_str(), it->second.name.length());

            storageStream.seekp(bioAddr, ios_base::beg);
            storageStream.write(it->second.bio.c_str(), it->second.bio.length());
        }
        storageStream.close();
    }

    Block* getLeastRecentBlock() {
        Block* result = &myBuffer[0];
        uint64_t leastRecent = globalLRU;
        for (int i = 0; i < NUM_BLOCKS; i++) {
            if (myBuffer[i].lru < leastRecent) {
                leastRecent = myBuffer[i].lru;
                result = &myBuffer[i];
            }
        }
        return result;
    }

public:
    int TotalRecords = 0;   // TODO: Revise the necessity of this variable

    StorageBufferManager(string NewFileName) {
        //initialize your variables
        myBuffer = new Block[NUM_BLOCKS];
        myStorageName = NewFileName;

        // Create your EmployeeRelation file 
        remove(NewFileName.c_str());
        storageStream.open(myStorageName, ios::binary | ios::out);
        storageStream.close();
    }

    ~StorageBufferManager() { }

    // Insert new record 
    void insertRecord(Record record, int pos) {
        //// The block are initialized in constructor
        //myIndexManager.myIndexRecord[record.id] = numRecords;

        // Add record to the block
        bool insertionSuccess = false;
        for (int i = 0; i < NUM_BLOCKS; i++) {
            insertionSuccess = myBuffer[i].insertRecord(&record, pos);
            if (insertionSuccess) {
                break;
            }
        }

        // Take neccessary steps if capacity is reached (you've utilized all the blocks in main memory)
        // if there are no space in buffer, release one page
        if (!insertionSuccess) {
            Block* blockToSave = getLeastRecentBlock();
            map<int, Record> recs = blockToSave->getAllRecords();
            saveRecords(&recs);
            blockToSave->clearBlock();
            blockToSave->insertRecord(&record, pos);
        }
    }

    void saveCache() {
        for (int i = 0; i < NUM_BLOCKS; i++) {
            map<int, Record> recs = myBuffer[i].getAllRecords();
            saveRecords(&recs);
            myBuffer[i].dirty = false;
        }
    }

    // Given an ID, find the relevant record and print it
    // I'm not proud of this code, but have lack of time for optimization...
    Record findRecordById(int id, int pos) {
        // Since this is supposed to be called by Index Manager,
        // we are certain we have the record
        // Looking at cache first
        for (int i = 0; i < NUM_BLOCKS; i++) {
            vector<uint64_t> recIds = myBuffer[i].getAllIds();
            for (int j = 0; j < recIds.size(); j++) {
                if (recIds[j] == id) {
                    return myBuffer[i].getRecordById(recIds[j]);
                }
            }
        }

        // Didn't find in cache, fetching from disk
        // Get the last frequently used page
        Block* blockToReplace = getLeastRecentBlock();
        // Save the block if it's dirty...
        //if (blockToReplace->dirty) {
        //    saveCache();
        //}
        blockToReplace->clearBlock();

        streamoff offset = pos * REC_PHYS_SIZE;
        ifstream ifs;
        //storageStream.open(myStorageName, ios::binary | ios::in);
        ifs.open(myStorageName, ios::binary);

        bool insertionSuccess = false;
        do {
            //storageStream.seekg(offset);
            ifs.seekg(offset);
            char recordCast[REC_PHYS_SIZE];
            char* addr = recordCast;
            //storageStream.read(addr, sizeof(uint64_t));
            ifs.read(addr, sizeof(uint64_t));
            uint64_t uid = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint16_t));
            uint16_t nameLength = *reinterpret_cast<uint16_t*>(addr);
            addr += sizeof(uint16_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint32_t));
            uint32_t nameAddr = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint16_t));
            uint16_t bioLength = *reinterpret_cast<uint16_t*>(addr);
            addr += sizeof(uint16_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint32_t));
            uint32_t bioAddr = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            //storageStream.read(addr, sizeof(uint64_t));
            ifs.read(addr, sizeof(uint64_t));
            uint64_t managerId = *reinterpret_cast<uint64_t*>(addr);

            char* nameFromCache = (char*)malloc((size_t)nameLength);
            //storageStream.seekg(nameAddr);
            //storageStream.read(nameFromCache, nameLength);
            ifs.seekg(nameAddr);
            ifs.read(nameFromCache, nameLength);

            char* bioFromCache = (char*)malloc((size_t)bioLength);
            //storageStream.read(bioFromCache, bioLength);
            ifs.read(bioFromCache, bioLength);

            Record r = Record(uid, convertToString(nameFromCache, nameLength), convertToString(bioFromCache, bioLength), managerId);

            insertionSuccess = blockToReplace->insertRecord(&r, pos);
            offset += REC_PHYS_SIZE;
        } while (insertionSuccess && offset < TotalRecords * REC_PHYS_SIZE);

        //storageStream.close();
        ifs.close();

        return blockToReplace->getRecordById(id);

        //// If there is record, it would have been returned by now
        //cout << "Record with this Id is not found\n";
        //return Record(id, "Not", "Found", 0);
    }

    // Had to do this, because stream works poorly.
    string convertToString(char* a, int size)
    {
        int i;
        string s = "";
        for (i = 0; i < size; i++) {
            s = s + a[i];
        }
        return s;
    }
};

class Bucket {
public:
    int localId;
    int* hashes = new int[BUCKET_CAPACITY];
    int* keys = new int[BUCKET_CAPACITY];
    bool* bitIsFlipped = new bool[BUCKET_CAPACITY];
    int* references = new int[BUCKET_CAPACITY];
    int i;

    // Pointer to the overflow bucket
    int nextBucketId = MAGIC_INT_EMPTY;

    Bucket() {
        for (int k = 0; k < BUCKET_CAPACITY; k++) {
            hashes[k] = MAGIC_INT_EMPTY;
            keys[k] = MAGIC_INT_EMPTY;
            bitIsFlipped[k] = false;
            references[k] = MAGIC_INT_EMPTY;
        }
    }
};

class LinearHashIndex {
private:
    // The Storage Buffer manager takes care of physical records read/write and blocks in memory.
    // Here I'm only taking care of buckets cache and its proper saved to / read from index file.

    //const int BLOCK_SIZE = 4096;
    const static int B_Size = 4096 - 128;    // One block for Index Buffer (2 blocks for storage buffer), approx 128 bytes for overhead
    vector<int> blockDirectory; // Map the least-significant-bits of h(id) to a bucket location in EmployeeIndex (e.g., the jth bucket)
                                // can scan to correct bucket using j*BLOCK_SIZE as offset (using seek function)
                                // can initialize to a size of 256 (assume that we will never have more than 256 regular (i.e., non-overflow) buckets)
    int n;      // The number of indexes in blockDirectory currently being used
    int i = 0;	// The number of least-significant-bits of h(id) to check. Will need to increase i once n > 2^i
    int numRecords = 0;    // Records currently in index. Used to test whether to increase n
    //int nextFreeBlock; // Next place to write a bucket. Should increment it by BLOCK_SIZE whenever a bucket is written to EmployeeIndex
    string fName;      // Name of index file

    int overflowBuckets = 0;
    StorageBufferManager* manager;
    int nextFree;
    char data[B_Size];

    // Hash function from the requirements
    int hashFunction(int key)
    {
        return key & 0b1111111111111111;
    }

    // Bit flip
    int bitFlip(int val, int bitIndex)
    {
        int mask = 1 << bitIndex - 1;
        int flip = ~(val & mask);

        return (val & (~mask)) & flip;
    }

    // Insert new record into index
    void insertRecord(Record record) {

        // No records written to index yet
        //if (numRecords == 0) {
            // Initialize index with first blocks (start with 4)

        //}

        // Add record to the index in the correct block, creating a overflow block if necessary
        int hash = hashFunction(record.id);
        int insertionKey = hash & (1 << i) - 1;
        bool bitIsFlipped = false;
        if (insertionKey > n - 1) {
            insertionKey = bitFlip(insertionKey, i);
            bitIsFlipped = true;
        }
        // Inserting...
        // Reading bucket from disk
        Bucket* b = getBucketFromCache(blockDirectory[insertionKey]);

        // tryInsert checks if the bucket has room, and saves to disk internally
        tryInsert(b, record.id, hash, bitIsFlipped, numRecords);

        //saveToDisk(b);
        manager->insertRecord(record, numRecords);
        numRecords++;

        // Take neccessary steps if capacity is reached:
        // increase n; increase i (if necessary); place records in the new bucket that may have been originally misplaced due to a bit flip
        if (100 * numRecords / (BUCKET_CAPACITY * (n + overflowBuckets)) > MAX_CAPACITY_THRESHOLD) {
            n++;
            int newi = 0;
            int t = n - 1;
            while (t > 0) {
                t = t >> 1;
                newi++;
            }

            // In any case, the new bucket is created and inserted into the array
            Bucket* newb = new Bucket();
            newb->i = newi;
            newb->localId = globalBucketId;
            globalBucketId++;
            // ****
            blockDirectory.push_back(newb->localId);
            // Don't forget to save new bucket to disk
            saveBucketToCache(newb);

            for (int k = 0; k < n - 1; k++) {
                Bucket* cb;
                int bref = blockDirectory[k];

                while (bref != MAGIC_INT_EMPTY) {
                    cb = getBucketFromCache(bref);

                    for (int q = 0; q < BUCKET_CAPACITY; q++) {
                        if (cb->keys[q] != MAGIC_INT_EMPTY) {
                            // Here, there are several scenarios happen.
                            // 1:
                            if (newi > i) {
                                // i has increased. Checking if records have new placements
                                // recomputing the bucket number:
                                int cKey = cb->hashes[q] & (1 << newi) - 1;
                                bool cIsBitflipped = false;
                                if (cKey > n - 1) {
                                    // Guaranteed bitflip
                                    cKey = bitFlip(cKey, newi);
                                    cIsBitflipped = true;
                                }
                                if (cKey == k) {
                                    // the rec is in the right bucket, save record if bitflipped has changed
                                    if (cb->bitIsFlipped[q] != cIsBitflipped) {
                                        cb->bitIsFlipped[q] = cIsBitflipped;
                                        saveBucketToCache(cb);
                                    }
                                    // no need to save if nothing changed
                                }
                                else {
                                    // moving to another bucket and erasing from the current
                                    Bucket* bDebug = getBucketFromCache(blockDirectory[cKey]);
                                    tryInsert(bDebug, cb->keys[q], cb->hashes[q], cIsBitflipped, cb->references[q]);
                                    cb->keys[q] = MAGIC_INT_EMPTY;
                                    cb->hashes[q] = MAGIC_INT_EMPTY;
                                    cb->bitIsFlipped[q] = false;
                                    cb->references[q] = MAGIC_INT_EMPTY;
                                    saveBucketToCache(cb);
                                }
                            }

                            // 2.
                            else {
                                // i hasn't increased. Checking records that may be inserted into new bucket
                                // if bit flip belongs to the new bucket, inserting it there
                                if (cb->bitIsFlipped[q] && (cb->hashes[q] & (1 << i) - 1) == n - 1) {
                                    tryInsert(newb, cb->keys[q], cb->hashes[q], false, cb->references[q]);
                                    cb->keys[q] = MAGIC_INT_EMPTY;
                                    cb->hashes[q] = MAGIC_INT_EMPTY;
                                    cb->bitIsFlipped[q] = false;
                                    cb->references[q] = MAGIC_INT_EMPTY;
                                    saveBucketToCache(cb);
                                }
                            }
                        }
                    }

                    bref = cb->nextBucketId;
                }
            }

            // After we finished rearranging buckets, don't forget to update i
            i = newi;
        }
    }

    // Inserts data to bucket, automatically fetches or creates overflow bucket
    void tryInsert(Bucket* bp, int keyp, int hashp, bool flipped, int refers) {
        for (int k = 0; k < BUCKET_CAPACITY; k++) {
            if (bp->keys[k] == MAGIC_INT_EMPTY) {
                bp->hashes[k] = hashp;
                bp->keys[k] = keyp;
                bp->bitIsFlipped[k] = flipped;
                bp->references[k] = refers;
                saveBucketToCache(bp);
                return;
            }
        }

        // no room in the bucket, checking if there are overflow buckets, and insert there
        Bucket* overb;
        if (bp->nextBucketId == MAGIC_INT_EMPTY) {
            // Creating overflow bucket
            overb = new Bucket();
            overb->i = i;
            overb->localId = globalBucketId;
            globalBucketId++;
            overflowBuckets++;
            // ****
            bp->nextBucketId = overb->localId;
            saveBucketToCache(bp);
        }
        else {
            // fetch overflow bucket from disk
            overb = getBucketFromCache(bp->nextBucketId);
        }
        tryInsert(overb, keyp, hashp, flipped, refers);
    }

    void clearCache() {
        nextFree = 0;
        memset(data, MAGIC_INT_EMPTY, B_Size);
    }

    void saveCacheToDisk() {
        fstream indexStream;
        indexStream.open(fName, ios::binary | ios::in | ios::out);

        for (char* addr = data; addr < data + nextFree; addr += BUCKET_PHYS_SIZE) {
            int pos = *reinterpret_cast<int*>(addr);
            streamoff offset = pos * BUCKET_PHYS_SIZE;
            indexStream.seekp(offset, ios_base::beg);
            indexStream.write(addr, BUCKET_PHYS_SIZE);
        }

        indexStream.close();
    }

    // Puts bucket to cache
    void saveBucketToCache(Bucket* b) {
        // Check if bucket is in the cache
        char* t = data;
        for ( ; t < data + nextFree; )
        {
            int id = *reinterpret_cast<int*>(t);
            if (id == b->localId) {
                // local id (skip)
                t += sizeof(int);

                // hashes
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    *reinterpret_cast<int*>(t) = b->hashes[k];
                    t += sizeof(int);
                }

                // keys
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    *reinterpret_cast<int*>(t) = b->keys[k];
                    t += sizeof(int);
                }

                // bit is flipped
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    *reinterpret_cast<int*>(t) = (int)b->bitIsFlipped[k];
                    t += sizeof(int);
                }

                // references
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    *reinterpret_cast<int*>(t) = b->references[k];
                    t += sizeof(int);
                }

                // i
                *reinterpret_cast<int*>(t) = b->i;
                t += sizeof(int);

                // next bucket reference
                *reinterpret_cast<int*>(t) = b->nextBucketId;
                t += sizeof(int);
                return;
            }
            else {
                t += BUCKET_PHYS_SIZE;
            }
        }

        if (B_Size - nextFree < BUCKET_PHYS_SIZE) {
            saveCacheToDisk();
            clearCache();
            t = data;
        }

        // local id
        *reinterpret_cast<int*>(t) = b->localId;
        t += sizeof(int);

        // hashes
        for (int k = 0; k < BUCKET_CAPACITY; k++) {
            *reinterpret_cast<int*>(t) = b->hashes[k];
            t += sizeof(int);
        }

        // keys
        for (int k = 0; k < BUCKET_CAPACITY; k++) {
            *reinterpret_cast<int*>(t) = b->keys[k];
            t += sizeof(int);
        }

        // bit is flipped
        for (int k = 0; k < BUCKET_CAPACITY; k++) {
            *reinterpret_cast<int*>(t) = (int)b->bitIsFlipped[k];
            t += sizeof(int);
        }

        // references
        for (int k = 0; k < BUCKET_CAPACITY; k++) {
            *reinterpret_cast<int*>(t) = b->references[k];
            t += sizeof(int);
        }

        // i
        *reinterpret_cast<int*>(t) = b->i;
        t += sizeof(int);

        // next bucket reference
        *reinterpret_cast<int*>(t) = b->nextBucketId;
        t += sizeof(int);

        nextFree += BUCKET_PHYS_SIZE;
    }

    // Gets bucket from cache. If it's not in the cache, reads it from disk and populates new cache
    Bucket* getBucketFromCache(int localId) {
        for (char* addr = data; addr < data + nextFree; )
        {
            int id = *reinterpret_cast<int*>(addr);

            if (id == localId) {
                Bucket* b = new Bucket();

                // local id
                b->localId = id;
                addr += sizeof(int);

                // hashes
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    b->hashes[k] = *reinterpret_cast<int*>(addr);
                    addr += sizeof(int);
                }

                // keys
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    b->keys[k] = *reinterpret_cast<int*>(addr);
                    addr += sizeof(int);
                }

                // bit is flipped
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    b->bitIsFlipped[k] = (bool)*reinterpret_cast<int*>(addr);
                    addr += sizeof(int);
                }

                // references
                for (int k = 0; k < BUCKET_CAPACITY; k++) {
                    b->references[k] = *reinterpret_cast<int*>(addr);
                    addr += sizeof(int);
                }

                // i
                b->i = *reinterpret_cast<int*>(addr);
                addr += sizeof(int);

                // next bucket reference
                b->nextBucketId = *reinterpret_cast<int*>(addr);
                addr += sizeof(int);

                return b;
            }
            else {
                addr += BUCKET_PHYS_SIZE;
            }
        }

        // if not found in cache, fetching new cache from disk
        saveCacheToDisk();
        clearCache();
        ifstream ifs;
        ifs.open(fName, ios::binary);

        for (int currentId = localId; currentId - localId + 1 <= B_Size / BUCKET_PHYS_SIZE && currentId < globalBucketId; currentId++)
        {
            // read bucket
            streamoff offset = currentId * BUCKET_PHYS_SIZE;
            ifs.seekg(offset, ios_base::beg);
            char* addr = new char[BUCKET_PHYS_SIZE];

            Bucket* b = new Bucket;

            // local id
            ifs.read(addr, sizeof(int));
            b->localId = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            // hashes
            for (int k = 0; k < BUCKET_CAPACITY; k++) {
                ifs.read(addr, sizeof(int));
                b->hashes[k] = *reinterpret_cast<int*>(addr);
                addr += sizeof(int);
            }

            // keys
            for (int k = 0; k < BUCKET_CAPACITY; k++) {
                ifs.read(addr, sizeof(int));
                b->keys[k] = *reinterpret_cast<int*>(addr);
                addr += sizeof(int);
            }

            // bit is flipped
            for (int k = 0; k < BUCKET_CAPACITY; k++) {
                ifs.read(addr, sizeof(int));
                b->bitIsFlipped[k] = (bool)*reinterpret_cast<int*>(addr);
                addr += sizeof(int);
            }

            // references
            for (int k = 0; k < BUCKET_CAPACITY; k++) {
                ifs.read(addr, sizeof(int));
                b->references[k] = *reinterpret_cast<int*>(addr);
                addr += sizeof(int);
            }

            // i
            ifs.read(addr, sizeof(int));
            b->i = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            // next bucket reference
            ifs.read(addr, sizeof(int));
            b->nextBucketId = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            saveBucketToCache(b);
        }

        ifs.close();
        return getBucketFromCache(localId);
    }

public:
    LinearHashIndex(string indexFileName) {
        n = 4; // Start with 4 buckets in index
        // It's better to manage "i" internally, since it's unambiguosly defined by n
        //i = 2; // Need 2 bits to address 4 buckets
        int t = n - 1;
        while (t > 0) {
            t = t >> 1;
            i++;
        }

        numRecords = 0;
        //nextFreeBlock = 0;
        fName = indexFileName;
        clearCache();

        manager = new StorageBufferManager(DATA_FILE_NAME);

        // Create your EmployeeIndex file and write out the initial 4 buckets
        // make sure to account for the created buckets by incrementing nextFreeBlock appropriately

        // Creating index table file 
        remove(indexFileName.c_str());
        fstream indexStream;
        indexStream.open(indexFileName, ios::binary | ios::out);
        indexStream.close();

        // Creating some initial stuff
        for (int k = 0; k < n; k++) {
            Bucket* b = new Bucket();
            b->i = i;
            b->localId = globalBucketId;
            globalBucketId++;
            saveBucketToCache(b);
            // ****
            blockDirectory.push_back(b->localId);
        }
    }

    // Read csv file and add records to the index
    void createFromFile(string csvFName) {
        ifstream in_file;

        in_file.open(csvFName, ios::in);

        string line, field;
        vector<string> fields;

        // grab entire line
        int c = 0;
        while (getline(in_file, line, '\n'))
        {
            // turn line into a stream
            stringstream s(line);

            // gets everything in stream up to comma
            fields.clear();
            getline(s, field, ',');
            fields.push_back(field);
            getline(s, field, ',');
            fields.push_back(field);
            getline(s, field, ',');
            fields.push_back(field);
            getline(s, field, ',');
            fields.push_back(field);

            Record r = Record(fields);
            insertRecord(r);
        }
        in_file.close();

        // TODO: I'll refactor this by next assignment
        saveCacheToDisk();
        manager->saveCache();
        manager->TotalRecords = numRecords;
    }

    // Given an ID, find the relevant record and print it
    Record findRecordById(int id) {
        int hash = hashFunction(id);
        int key = hash & (1 << i) - 1;
        bool bitIsFlipped = false;
        if (key > n - 1) {
            key = bitFlip(key, i);
            bitIsFlipped = true;
        }

        // Reading bucket from disk
        Bucket* b;
        int bref = blockDirectory[key];

        do {
            b = getBucketFromCache(bref);
            for (int q = 0; q < BUCKET_CAPACITY; q++) {
                if (b->hashes[q] == hash) {
                    return manager->findRecordById(b->keys[q], b->references[q]);
                }
            }
            bref = b->nextBucketId;
        } while (bref != MAGIC_INT_EMPTY);

        return Record(id, "Not", "Found", 0);
    }
};
