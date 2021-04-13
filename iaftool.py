from __future__ import print_function

import os
import sys
import bz2
import zlib
import codecs
import pickle
import errno
import random

__author__ = "isacolak04@gmail.com"
__version__ = "0.1"

def bytesto(bytes, to, bsize=1024):
	a = {1: "kb", 2: "mb", 3: "gb", 4: "tb", 5: "pb", 6: "eb"}
	aa = {'kb' : 1, 'mb': 2, 'gb' : 3, 'tb' : 4, 'pb' : 5, 'eb' : 6 }
	if isinstance(to, str):
		to = aa[to]
	elif isinstance(to, int):
		to = to

	r = float(bytes)
	m = bytes / (bsize ** to)

	if m < 0.01 and to > 0:
		while m < 0.01:
			to -= 1
			m  = bytesto(bytes, to)[0]
	
	if m > 999 and to <= 6:
		while m > 999:
			to += 1
			m = bytesto(bytes, to)[0]
	return m, a[to]

def floatnumberformatter(number):
	ms = str(number)
	msf = "{number2:,}".format(number2=int(ms[:ms.index(".")]))
	msf += ms[ms.index("."):][:3]
	return msf

if sys.version_info[0] >= 3:
	def _unicode(text):
		return text

	def _printable(text):
		return text

	def _unmangle(data):
		return data.encode('latin1')

	def _unpickle(data):
		return pickle.loads(data, encoding='latin1')
elif sys.version_info[0] == 2:
	def _unicode(text):
		if isinstance(text, unicode):
			return text
		return text.decode('utf-8')

	def _printable(text):
		return text.encode('utf-8')

	def _unmangle(data):
		return data

	def _unpickle(data):
		return pickle.loads(data)

class iArchive:
	file  = None
	handle = None

	files = {}
	indexes = {}

	version = None
	verbose = False

	iA_V1 = "iA-1.0"

	PICKLE_PROTOCOL = 2

	def __init__(self, file=None,version=1,verbose=False,compression_library="bz2"):
		self.compression_library = compression_library
		self.verbose = verbose

		if file is not None:
			self.load(file)
		else:
			self.version = version

	def get_version(self):
		self.handle.seek(0)

		version = self.handle.readline().decode("utf-8")

		if version.startswith(self.iA_V1):
			return 1

		raise ValueError("File entered is not archive or unsupported version.")

	def verbose_print(self, msg):
		if self.verbose:
			print(msg)

	def list(self):
		fileslist = []

		for file in self.files:
			filesize = bytesto(self.files[file]["len"], "mb")
			fileslist.append("{0} ({1} {2})".format(file,floatnumberformatter(filesize[0])[:4],filesize[1]))

		return fileslist
	
	def list_filesnames(self):
		return list(self.files.keys())
			
	def convert_filename(self, filename):
		(drive, filename) = os.path.splitdrive(os.path.normpath(filename).replace(os.sep, '/'))
		return filename

	def extract_files(self):
		self.handle.seek(0)

		files = None

		metadata = self.handle.readline()
		vals = metadata.split()
		offset = int(vals[1],16)

		self.handle.seek(offset)
		try:
			contents = codecs.decode(self.handle.read(), self.compression_library)
		except Exception as e:
			raise IOError(e)
		files = _unpickle(contents)

		return files

	def add(self,filename,contents,filesize):
		filename = self.convert_filename(_unicode(filename))

		if filename in self.files:
			raise ValueError("{0} is already in the archive.".format(_printable(filename)))

		self.verbose_print('{0} was added to the archive. (len = {1} mb)'.format(_printable(filename), filesize))
		self.files[filename] = {"contents":contents,"len":filesize}

	def remove(self,filename):
		if filename in self.files:
			self.verbose_print("{0} is being removed from the archive.".format(_printable(filename)))
			del self.files[filename]
		else:
			raise IOError(errno.ENOENT, '{0} not found in archive.'.format(_printable(filename)))

	def change(self,filename,contents,filesize):
		filename = _unicode(filename)

		self.remove(filename)
		self.add(filename, contents)

	def read(self,filename):
		filename = self.convert_filename(_unicode(filename))

		if filename not in self.files:
			raise IOError(errno.ENOENT, '{0} not found in archive.'.format(_printable(filename)))

		if filename not in self.files and self.handle is None:
			raise IOError(errno.ENOENT, '{0} not found in archive.'.format(_printable(filename)))

		if filename in self.files:
			return self.files[filename]["contents"]

	def load(self,filename):
		filename = _unicode(filename)

		if self.handle is not None:
			self.handle.close()
		self.file = filename
		self.files = {}
		self.handle = open(self.file,"rb")
		self.version = self.get_version()
		self.files = self.extract_files()

	def save(self, filename):
		filename = _unicode(filename)

		if filename is None:
			filename = self.file
		if filename is None:
			raise ValueError("No file to save archive found.")
		if self.version != 1:
			raise ValueError('Saving is only supported for version 1 archives.')

		self.verbose_print('Rebuilding archive index...')
		files = self.files

		offset = 0
		if self.version == 1:
			offset = 25

		archive = open(filename,"wb")
		archive.seek(offset)

		self.verbose_print('Writing files to archive file...')
		self.verbose_print('Writing archive index to archive file...')
		archive.write(codecs.encode(pickle.dumps(files, self.PICKLE_PROTOCOL), self.compression_library))

		archive.seek(0)
		if self.version == 1:
			self.verbose_print('Writing header to archive file... (version = {0})'.format(self.iA_V1))
			header = "{} {:016x}\n".format(self.iA_V1,offset)
			archive.write(codecs.encode(header))

		archive.close()

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(
		description="A tool for working with iArchive files.",
		epilog='The FILE argument can optionally be in ARCHIVE=REAL format, mapping a file in the archive file system to a file on your real file system. An example of this: iaftool -x test.iaf test.txt=/home/foo/test.txt',
		add_help=False)

	parser.add_argument('archive', metavar='ARCHIVE', help='The iArchive file to process.')
	parser.add_argument('files', metavar='FILE', nargs='*', action='append', help='Zero or more files to operate on.')

	parser.add_argument('-l', '--list', action='store_true', help='List files in ARCHIVE.')
	parser.add_argument('-x', '--extract', action='store_true', help='Extract FILEs from ARCHIVE.')
	parser.add_argument('-c', '--create', action='store_true', help='Creative ARCHIVE from FILEs.')
	parser.add_argument('-d', '--delete', action='store_true', help='Delete FILEs from ARCHIVE.')
	parser.add_argument('-a', '--append', action='store_true', help='Append FILEs to ARCHIVE.')

	parser.add_argument('-o', '--outfile', help='An alternative output archive file when appending to or deleting from archives, or output directory when extracting.')
	parser.add_argument('-v', '--verbose', action='store_true', help='Be a bit more verbose while performing operations.')
	
	parser.add_argument('-z', '--zlib', action='store_true', help='Select "zlib" library for compress.')
	parser.add_argument('-b', '--bz2', action='store_true', help='Select "bz2" library for compress.')
	parser.add_argument('-cl', '--compression_library', help='Select compression library.')

	parser.add_argument('-h', '--help', action='help', help='Print this help and exit.')
	parser.add_argument('-V', '--version', action='version', version='iaftool v0.1', help='Show version information.')

	args = parser.parse_args()

	version = 1

	if args.create:
		archive = None
		output = _unicode(args.archive)
	else:
		archive = _unicode(args.archive)

		if 'outfile' in args and args.outfile is not None:
			output = _unicode(args.outfile)
		else:
			if args.extract:
				output = "."
			else:
				output = _unicode(args.archive)

	if len(args.files) > 0 and isinstance(args.files[0], list):
		args.files = args.files[0]

	try:
		if args.zlib:
			archive = iArchive(archive, verbose=args.verbose, compression_library="zlib")
		elif args.bz2:
			archive = iArchive(archive, verbose=args.verbose, compression_library="bz2")
		elif args.compression_library is not None:
			archive = iArchive(archive, verbose=args.verbose, compression_library=args.compression_library)
		else:
			archive = iArchive(archive, verbose=args.verbose)
		
	except IOError as e:
		print('Could not open archive file {0} for reading: {1}'.format(archive, e), file=sys.stderr)
		sys.exit(1)

	if args.create or args.append:
		def add_file(filename):
			if filename.find('=') != -1:
				(outfile, filename) = filename.split('=', 2)
			else:
				outfile = filename

			if os.path.isdir(filename):
				for file in os.listdir(filename):
					add_file(outfile + os.sep + file + '=' + filename + os.sep + file)
			else:
				try:
					with open(filename, 'rb') as file:
						filecontents = file.read()
						filesize = sys.getsizeof(filecontents)
						archive.add(outfile, filecontents, filesize)
				except Exception as e:
					print('Could not add file {0} to archive: {1}'.format(filename, e), file=sys.stderr)

		for filename in args.files:
			add_file(_unicode(filename))

		archive.version = version
		
		try:
			archive.save(output)
		except Exception as e:
			print('Could not save archive file: {0}'.format(e), file=sys.stderr)

	elif args.delete:
		for filename in args.files:
			try:
				archive.remove(filename)
			except Exception as e:
				print('Could not delete file {0} from archive: {1}'.format(filename, e), file=sys.stderr)
				print('Archive file is being saved.', file=sys.stderr)

		archive.version = version

		try:
			archive.save(output)
		except Exception as e:
			print('Could not save archive file: {0}'.format(e), file=sys.stderr)

	elif args.extract:
		if len(args.files) > 0:
			files = args.files
		else:
			files = archive.list_filesnames()

		if not os.path.exists(output):
			os.makedirs(output)

		for filename in files:
			if filename.find('=') != -1:
				(outfile, filename) = filename.split('=', 2)
			else:
				outfile = filename

			try:
				contents = archive.read(filename)

				if not os.path.exists(os.path.dirname(os.path.join(output, outfile))):
					os.makedirs(os.path.dirname(os.path.join(output, outfile)))

				with open(os.path.join(output, outfile), 'wb') as file:
					file.write(contents)
				
				archive.verbose_print("Extracted file {0} from archive.".format(os.path.join(output, outfile)), file=sys.stdout)
			except Exception as e:
				print('Could not extract file {0} from archive: {1}'.format(filename, e), file=sys.stderr)

	elif args.list:
		list = archive.list()
		list.sort()
		for file in list:
			print(file)
	else:
		print('No operation given :(')
		print('Use iaftool --help for usage details.')