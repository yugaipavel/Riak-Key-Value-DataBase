import riak, string, random, time
import matplotlib.pyplot as plt
from os import mkdir
#read write read readable writeable full modify

count_of_rows = [1, 10, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 15000, 20000, 25000, 30000]

BucketName = 'NoSQL_Laba3'

def randomword(length):
	# get a random string from lowercase latin characters
    
	letters = string.ascii_lowercase
	return ''.join(random.choice(letters) for i in range(length))

def init_data(key_list, value_list, count_of_rows):
	# get a random number and a random string, fill in the list of values for key_list and value_list
    
	for i in range(count_of_rows):
		num = random.randint(1, 100000000)
		str = randomword(80)

		key_list.append(num)
		value_list.append(str)

def create_new_bucket():
	client = riak.RiakClient()
	return client, client.bucket(BucketName)

def delete_bucket_and_all_keys_from_it(Client, MyBucket):
	for keys in MyBucket.stream_keys():
		for key in keys:
			#print('Deleting %s' % key)
			MyBucket.delete(key)
	#print(MyBucket.get_keys())
	#print(Client.get_buckets())

def test_create_key_value():
	key_list = []
	value_list = []

	file_create_key_value = open('report_NoSQL/file_create_key_value.txt', 'w')
	file_get_key_value = open('report_NoSQL/file_get_key_value.txt', 'w')
	file_update_key_value = open('report_NoSQL/file_update_key_value.txt', 'w')

	FOR_GRAPH_file_create_key_value = open('report_NoSQL/FOR_GRAPH_file_create_key_value.txt', 'w')
	FOR_GRAPH_file_get_key_value = open('report_NoSQL/FOR_GRAPH_file_get_key_value.txt', 'w')
	FOR_GRAPH_file_update_key_value = open('report_NoSQL/FOR_GRAPH_file_update_key_value.txt', 'w')


	Client, MyBucket  = create_new_bucket()

	for local_count in count_of_rows:
		init_data(key_list, value_list, local_count)

		file_create_key_value.write("-----Count of rows: %d-----\n"%local_count)
		file_get_key_value.write("-----Count of rows: %d-----\n"%local_count)
		file_update_key_value.write("-----Count of rows: %d-----\n"%local_count)

		average_create_key_value = 0
		average_get_key_value = 0
		average_update_key_value = 0

		for j in range(6):
			start_time = time.time()
			for i in range(local_count-1):
				NewObject = MyBucket.new(str(key_list[i]), data=str(value_list[i]))
				NewObject.store()
			NewObject = MyBucket.new('917591', data='hahahahaha')
			NewObject.store()
			local_time = time.time() - start_time
			average_create_key_value += local_time
			file_create_key_value.write(str(local_time) + ', ')

			start_time = time.time()
			res = MyBucket.get('917591')
			local_time = time.time() - start_time
			average_get_key_value += local_time
			file_get_key_value.write(str(local_time) + ', ')

			start_time = time.time()
			res.data = 'gggggggggg'
			local_time = time.time() - start_time
			average_update_key_value += local_time
			file_update_key_value.write(str(local_time) + ', ')
		#print(res.encoded_data)

		file_create_key_value.write("\nAverage time: %s\n\n"%str(average_create_key_value/(6*local_count)))
		file_get_key_value.write("\nAverage time: %s\n\n"%str(average_get_key_value/6))
		file_update_key_value.write("\nAverage time: %s\n\n"%str(average_update_key_value/6))
		
		FOR_GRAPH_file_create_key_value.write(str(average_create_key_value/(6*local_count)) + ', ')
		FOR_GRAPH_file_get_key_value.write(str(average_get_key_value/6) + ', ')
		FOR_GRAPH_file_update_key_value.write(str(average_update_key_value/6) + ', ')

		delete_bucket_and_all_keys_from_it(Client, MyBucket)
		#print(MyBucket.get_keys())

def main():
	try:
		mkdir('report_NoSQL')
	except OSError as error:
		print('Directory \\report_NoSQL alredy exsists\n')
	test_create_key_value()

if __name__ == '__main__':
    main()