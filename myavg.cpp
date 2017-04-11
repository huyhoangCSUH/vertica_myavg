#include "Vertica.h"
#include <stdio.h>
#include <fstream>
#include <iostream>
using namespace Vertica;
using namespace std;

class SumEtCount : public TransformFunction {
	// Called for each partition in the table
	virtual void processPartition(ServerInterface &srvInterface,
		PartitionReader &inputReader,
		PartitionWriter &outputWriter) {

		try {
			int count = 0;
			vfloat sum = 0;
			union {
				unsigned long long binval;
				double dval;
			} myU;
			do {
				const vfloat newValue = inputReader.getFloatRef(0);
				sum += newValue;
				count++;
			} while (inputReader.next());
			outputWriter.setFloat(0, sum);
			outputWriter.setInt(1, count);	
			//uint64_t sumBin;
			//memcpy(&sumBin, &sum, sizeof(sum));
			myU.dval = sum;
			outputWriter.setDataArea(2, &myU);
			outputWriter.next();
			/*
			try {
				FILE *binFile;
				binFile = fopen("/home/vertica/test.bin", "wb");
				fwrite(&sum, sizeof(vfloat), sizeof(sum)/sizeof(vfloat), binFile);
				fwrite(&count, sizeof(int), 1, binFile);
				fclose(binFile);
				
				ofstream txtFile;
				txtFile.open("/home/vertica/test.txt", ios::out | ios::app);
				txtFile << "sum" << endl;
				txtFile << "count" << endl;;
				txtFile.close();

				ofstream fs("/home/vertica/test.bin", ios::out | ios::binary | ios::app);
				fs.write(reinterpret_cast<const char*>(&sum), sizeof sum);
				fs.write(reinterpret_cast<const char*>(&count), sizeof count);
				fs.close();
				srvInterface.log("sum: %g\n", sum);
				srvInterface.log("count: %d\n", count);
							
			}	catch (exception &e) {
				vt_report_error(0, "exception while making file: [%s]", e.what());
			}
			*/
		} catch (exception &e) {
			vt_report_error(0, "exception while processing: [%s]", e.what());
		}
	}
};

class SumEtCountFactory : public TransformFunctionFactory {
	virtual void getPrototype(ServerInterface &srvInterface, 
		ColumnTypes &argTypes, ColumnTypes &returnType) {
		argTypes.addFloat();
		returnType.addFloat();
		returnType.addInt();
		returnType.addBinary();
	}

	virtual void getReturnType(ServerInterface &srvInterface,
		const SizedColumnTypes &input_types,
		SizedColumnTypes &output_types) {
		output_types.addFloat("Sum");
		output_types.addInt("Count");
		output_types.addBinary(8, "Count in Bin");
	}

	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) {
		return vt_createFuncObj(srvInterface.allocator, SumEtCount);
	}
};

RegisterFactory(SumEtCountFactory);