import random

#TEST FUNCTION
#Parses single datum into single record
def parse_test_single(test_data):
	random.seed(str(test_data))
	result = {
		"rand1" : random.random(),
		"rand2" : random.random(),
		"randint" : random.randint(0, 10),
		"composition" : "H2SO4"
		}
	return result

#TEST FUNCTION
#Parses single datum into list of records
def parse_test_list(test_data):
	try:
		count = int(test_data)
	except:
		count = 10
	return [parse_test_single(i) for i in range(count)]

