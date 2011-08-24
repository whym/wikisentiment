# run load_dataset.py and extract_features.py before running this
./split_dataset.py -H localhost -s '1,1,1,1,1' -r 1
./train.py --host localhost --find '{"internal.slice": {"$in": [0,1,2,3]}}' --model='{"cross-validation": 4}'
./test.py --host localhost --find '{"internal.slice": {"$in": [4]}}'  --model='{"cross-validation": 4}' -o output4.csv
./train.py --host localhost --find '{"internal.slice": {"$in": [0,1,2,4]}}' --model='{"cross-validation": 3}'
./test.py --host localhost --find '{"internal.slice": {"$in": [3]}}'  --model='{"cross-validation": 3}' -o output3.csv
./train.py --host localhost --find '{"internal.slice": {"$in": [0,1,3,4]}}' --model='{"cross-validation": 2}'
./test.py --host localhost --find '{"internal.slice": {"$in": [2]}}'  --model='{"cross-validation": 2}' -o output2.csv
./train.py --host localhost --find '{"internal.slice": {"$in": [0,2,3,4]}}' --model='{"cross-validation": 1}'
./test.py --host localhost --find '{"internal.slice": {"$in": [1]}}'  --model='{"cross-validation": 1}' -o output1.csv
./train.py --host localhost --find '{"internal.slice": {"$in": [1,2,3,4]}}' --model='{"cross-validation": 0}'
./test.py --host localhost --find '{"internal.slice": {"$in": [0]}}'  --model='{"cross-validation": 0}' -o output0.csv
./calc_performance.py output?.csv
