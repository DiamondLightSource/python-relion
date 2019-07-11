import mrcfile as mrc
import matplotlib.pyplot as plt

with mrc.open('/dls/ebic/data/staff-scratch/Donovan/relionTutor2/relion30_tutorial/MotionCorr/job003/Movies/20170629_00021_frameImage.mrc') as doc:
	im = doc.data
plt.imshow(im)
print('Shape: {}'.format(im.shape))
plt.show()
im_1 = im
for y in range(0, len(im))
	for x in im[y,:]:
		if x < 4:
			
		
