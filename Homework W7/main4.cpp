// Evgeny Ovechnikov
// Course: CS540 - Database Management Systems
// Homework: Sorting on External Memory (w7)

/* This is a skeleton code for External Memory Sorting, you can make modifications as long as you meet
   all question requirements*/  

#include "record_class.h"

using namespace std;

//defines how many blocks are available in the Main Memory 
#define buffer_size 22
const string pass_filename = "pass";

Records buffers[buffer_size]; //use this class object of size 22 as your main memory

// Defining comparator for Records (by Employee Id)
int compareRecords(const void* a, const void* b)
{
    const Records* x = (Records*)a;
    const Records* y = (Records*)b;

    if (x->emp_record.eid > y->emp_record.eid) return 1;
    if (x->emp_record.eid < y->emp_record.eid) return -1;

    return 0;
}

/***You can change return type and arguments as you want.***/

//PASS 1
//Sorting the buffers in main_memory and storing the sorted records into a temporary file (Runs) 
void Sort_Buffer(Records* buf){
    //Remember: You can use only [AT MOST] 22 blocks for sorting the records / tuples and create the runs
    // Sorting by eid as requirement
    int numr = buf->number_of_emp_records;
    std::qsort(buf, buf->number_of_emp_records, sizeof(Records), compareRecords);
    buf->number_of_emp_records = numr;
    return;
}

//PASS 2
//Use main memory to Merge the Runs 
//which are already sorted in 'runs' of the relation Emp.csv 
int Merge_Runs(int order){
    //and store the Sorted results of your Buffer using PrintSorted() 

    fstream passIn;
    passIn.open(pass_filename + to_string(order - 1), ios::binary | ios::in);
    Records pRec, qRec;
    char* tp = new char[sizeof(Records)];
    char* tq = new char[sizeof(Records)];

    fstream passOut;
    passOut.open(pass_filename + to_string(order), ios::binary | ios::out);

    bool eof = false;
    int seg = 0;
    for ( ; !eof; seg++) {
        int p = 0;
        int q = buffer_size << (order - 1);
        while (p < buffer_size << (order - 1) && q < buffer_size << order) {
            passIn.seekp((seg * (buffer_size << order) + p) * sizeof(Records), ios_base::beg);
            passIn.read(tp, sizeof(Records));
            pRec = *reinterpret_cast<Records*>(tp);

            passIn.seekp((seg * (buffer_size << order) + q) * sizeof(Records), ios_base::beg);
            passIn.read(tq, sizeof(Records));
            qRec = *reinterpret_cast<Records*>(tq);

            if (passIn.eof()) {
                // right pointer reached the end of file
                // current q data is garbage. Exiting.
                eof = true;
                break;
            }

            if (pRec.emp_record.eid <= qRec.emp_record.eid) {
                passOut.write(tp, sizeof(Records));
                p++;
            }
            else {
                passOut.write(tq, sizeof(Records));
                q++;
            }
        }
        // Kludge, C++ file stream shenanigans.
        // I have to close/open when using EOF logic to read from that file again. :(
        if (eof) {
            passIn.close();
            passIn.open(pass_filename + to_string(order - 1), ios::binary | ios::in);
        }
        // Dealing with the leftovers...
        for (; p < buffer_size << (order - 1); p++) {
            passIn.seekp((seg * (buffer_size << order) + p) * sizeof(Records), ios_base::beg);
            passIn.read(tp, sizeof(Records));
            if (passIn.eof()) {
                eof = true;
                break;
            }
            passOut.write(tp, sizeof(Records));
        }
        for (; q < buffer_size << order && !eof; q++) {
            passIn.seekp((seg * (buffer_size << order) + q) * sizeof(Records), ios_base::beg);
            passIn.read(tq, sizeof(Records));
            if (passIn.eof()) {
                eof = true;
                break;
            }
            passOut.write(tq, sizeof(Records));
        }
    }

    passIn.close();
    passOut.close();

    return (seg == 1) ? order : Merge_Runs(order + 1);
}

void savePass0(Records* buf, unsigned int page) {
    fstream pass0;
    pass0.open(pass_filename + "0", ios::binary | ios::in | ios::out);
    pass0.seekp(page * sizeof(buffers), ios_base::beg);

    int nRecords = buf->number_of_emp_records;
    char* t = new char[sizeof(Records)];
    for (int i = 0; i < nRecords; i++) {
        *reinterpret_cast<Records*>(t) = buffers[i];
        pass0.write(t, sizeof(Records));
    }
    pass0.close();
}

void PrintSorted(int order, fstream& sortOut) {
    //Store in EmpSorted.csv
    fstream passN;
    passN.open(pass_filename + to_string(order), ios::binary | ios::in);
    sortOut.fixed;

    while (true)
    {
        char* t = new char[sizeof(Records)];
        passN.read(t, sizeof(Records));
        Records r = *reinterpret_cast<Records*>(t);

        // C++ EOF shenanigans.
        // Loop break has to be here to truncate the last portion of data (garbage).
        if (passN.eof()) break;

        sortOut << r.emp_record.eid << ","
            << r.emp_record.ename << ","
            << r.emp_record.age << ","
            << (long long)r.emp_record.salary << endl;
    }

    passN.close();
    return;
}

int main() {

    //Open file streams to read and write
    //Opening out the Emp.csv relation that we want to Sort
    fstream empin;
    empin.open("Emp.csv", ios::in);

    //Creating the EmpSorted.csv file where we will store our sorted results
    remove("EmpSorted.csv");
    fstream SortOut;
    SortOut.open("EmpSorted.csv", ios::out | ios::app);

    // Creating 0-length file for pass 0
    remove((pass_filename + "0").c_str());
    fstream pass0;
    pass0.open((pass_filename + "0"), ios::binary | ios::out);
    pass0.close();

    //1. Create runs for Emp which are sorted using Sort_Buffer()

    unsigned int pagesCount = 0;
    bool stoppingToken = false;
    do
    {
        // Handling some edge cases, when number of records is exact multiple of buffer capacity...
        for (int i = 0; i < buffer_size; i++) {
            Records r = Grab_Emp_Record(empin);
            if (r.no_values) {
                stoppingToken = true;
                buffers[0].number_of_emp_records = i;
                break;
            }
            else {
                buffers[i] = r;
                buffers[0].number_of_emp_records = i + 1;
            }
        }
        if (buffers[0].number_of_emp_records) {
            Sort_Buffer(buffers);
            savePass0(buffers, pagesCount);
            pagesCount++;
        }
    } while (!stoppingToken);

    //2. Use Merge_Runs() to Sort the runs of Emp relations 
    int res = Merge_Runs(1);
    PrintSorted(res, SortOut);

    //Please delete the temporary files (runs) after you've sorted the Emp.csv
    for (int j = 0; j <= res; j++) {
        remove((pass_filename + to_string(j)).c_str());
    }

    empin.close();
    SortOut.close();
    return 0;
}
