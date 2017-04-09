#include "Vertica.h"
#include <stdio.h>

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
			do {
				const vfloat newValue = inputReader.getFloatRef(0);
				sum += newValue;
				count++;
			} while (inputReader.next());
			outputWriter.setFloat(0, sum)
			outputWriter.setInt(1, count);
			FILE* binFile;
			binFile = fopen("/tmp/test.bin", "wb");
			cout << fwrite(sum, sizeof(vfloat), 1, binFile) << endl;
			cout << fwrite(count, sizeof(int), 1, binFile) << endl;
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
	}

	virtual void getReturnType(ServerInterface &srvInterface,
		const SizedColumnTypes &input_types,
		SizedColumnTypes &output_types) {
		output_types.addFloat("The Sum");
		output_types.addInt("The Count");
	}

	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) {
		return vt_createFuncObj(srvInterface.allocator, SumEtCount);
	}
};

RegisterFactory(SumEtCountFactory);