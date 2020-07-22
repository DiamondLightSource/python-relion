import FindData
from pprint import pprint
import relion.input_dict

# Input entries for the FindData class would be found once and entered here, then passed in as an argument when instantiating a FindData object.
# Currently part of the file path has to be included in the 'name' section, but this would ideally be sorted if we use this structre in the future.
# If this was the design used, something else to improve is the dictionary format
# - it can be hard to keep track of how many values are in each field, and it would feel more intuitive if each set of square brackets emcompassed one value and its corresponding fields.

first_data = FindData.FindData(
    "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults",
    relion.input_dict.input_star,
)

second_data = FindData.FindData(
    "/dls/m02/data/2020/bi27053-1/processing/Relion_nd", relion.input_dict.input_out
)

output = first_data.get_data()
pprint(output)

# pprint(second_data.get_data())
