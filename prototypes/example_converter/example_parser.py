import random

#TEST FUNCTION
#Input: single record
#Output: single record
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
#Input: single record
#Output: yields many records
def parse_test_list(test_data):
	try:
		count = int(test_data)
	except:
		count = 10
	for i in range(count):
		yield parse_test_single(i)
#	return [parse_test_single(i) for i in range(count)]

