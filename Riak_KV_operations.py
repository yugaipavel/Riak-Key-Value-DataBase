import riak

#read write read readable writeable full modify

def create_and_fill(MyObjectBucket):
	id_obj = input('Enter ID of object: ')
	ID_of_file_system = input('Enter ID of file system: ')
	name_of_object = input('Enter name of object: ')
	subject_or_object = input('Enter type (subject or object): ')
	type_obj = input('Enter type (folder, user, exe, txt and so on...): ')
	full_path = input('Enter full path: ')
	print()

	choice = -1
	list_of_ar = []
	while (int(choice)!=0):
		print('Add access rights:')
		print('0.Cancel.')
		print('1.Add access right.')
		choice = input('Choose a number: ')
		if (int(choice) == 1):
			access_right = input('Enter access right (read, write, readable, writeable, modify, full): ')
			sec_id_obf = input('Enter ID of the second object: ')
			list_of_ar.append([access_right, sec_id_obf])
			print()
		elif (int(choice)== 0):
			print()
			break
		else:
			print('Enter a number of [0-1]')
			print()
	new_obj = MyObjectBucket.new(id_obj, data={
	'ID_object': id_obj,
	'ID_of_file_system': ID_of_file_system,
	'name_of_object': name_of_object,
	'subject_or_object': subject_or_object,
	'type_of_object': type_obj,
	'full_path': full_path,
	'access_rights': list_of_ar})
	new_obj.store()
	print('Object successfully created!')

def change(MyObjectBucket):
	id_obj = input('Enter ID of object: ')
	FetchedObject = MyObjectBucket.get(str(id_obj))
	FetchedObject.clear()
	ID_of_file_system = input('Enter new ID of file system: ')
	name_of_object = input('Enter new name of object: ')
	subject_or_object = input('Enter new type (subject or object): ')
	type_obj = input('Enter new type (folder, user, exe, txt and so on...): ')
	full_path = input('Enter new full path: ')
	print()

	choice = -1
	list_of_ar = []
	while (int(choice)!=0):
		print('Add new access rights:')
		print('0.Cancel.')
		print('1.Add access right.')
		choice = input('Choose a number: ')
		if (int(choice) == 1):
			access_right = input('Enter access right (read, write, readable, writeable, modify, full): ')
			sec_id_obf = input('Enter ID of the second object: ')
			list_of_ar.append([access_right, sec_id_obf])
			print()
		elif (int(choice)== 0):
			print()
			break
		else:
			print('Enter a number of [0-1]')
			print()
	new_obj = MyObjectBucket.new(id_obj, data={
	'ID_object': id_obj,
	'ID_of_file_system': ID_of_file_system,
	'name_of_object': name_of_object,
	'subject_or_object': subject_or_object,
	'type_of_object': type_obj,
	'full_path': full_path,
	'access_rights': list_of_ar})
	new_obj.store()
	print('Object successfully changed!')

def delete(MyObjectBucket):
	id_obj = input('Enter ID of object: ')
	FetchedObject = MyObjectBucket.get(str(id_obj))
	FetchedObject.delete()

def print_data(MyObjectBucket):
	id_obj = input('Enter ID of object: ')
	FetchedObject = MyObjectBucket.get(str(id_obj))
	if (FetchedObject.encoded_data != None):
		print('Information about object with ID {0}:'.format(id_obj))
		print(FetchedObject.encoded_data)
	else:
		print('The object with ID {0} does not exist'.format(id_obj))
"""
def search_data(client, MyObjectBucket):
	key = input('Enter the key for searching: ')
	value = input('Enter the value for searching: ')
	query = key + ':' + value
	print('Your query: ', query)
	result = client.search(MyObjectBucket, query)
	print(result)
"""

def menu():
	print('Welcome to My DataBase of Key-Value')
	print('0.Exit.')
	print('1.Create object and fill it up.')
	print('2.Change the data in the bucket')
	print('3.Delete the bucket')
	print('4.Print data from the bucket')
	return input('Choose a number: ')

def main():
	client = riak.RiakClient()
	MyObjectBucket  = client.bucket('MyObjects')
	choice = -1
	while (int(choice)!=0):
		choice = menu()
		print()
		if (int(choice)== 1):
			print('************* Create and fill *************\n')
			create_and_fill(MyObjectBucket)
			print()
		elif (int(choice)== 2):
			print('***************** Change *****************\n')
			change(MyObjectBucket)
			print()
		elif (int(choice)== 3):
			print('***************** Delete *****************\n')
			delete(MyObjectBucket)
			print()
		elif (int(choice)== 4):
			print('****************** Print ******************\n')
			print_data(MyObjectBucket)
			print()
		elif (int(choice)== 0):
			print('*************** Exiting... ***************\n')
			break
		else:
			print('Enter a number of [0-4]')
			print()

if __name__ == '__main__':
    main()