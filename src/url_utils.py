from tornado import gen, process

import state

import re, logging, md5

logger = logging.getLogger('tornado.general')

@gen.coroutine
def get_canonical_url(url):

	# Check if the result is cached in the state
	hash_key = md5.new(url).hexdigest()
	result = yield state.get("canonical_url_%s" % hash_key)
	if result is not None:
		raise gen.Return(result)

	sub_process = process.Subprocess(
		args= "curl -sIL '%s' | grep -i ^location:" % url,
		shell= True,
		stdout= process.Subprocess.STREAM,
		stderr= process.Subprocess.STREAM)
	
	result, error = yield [
        gen.Task(sub_process.stdout.read_until_close),
        gen.Task(sub_process.stderr.read_until_close)
        ]
	logger.info("Subprocess result: " + str(result))
	logger.info("Subprocess error: " + str(error))

	result = result.splitlines()
	if len(result) >= 1:
		result = result[-1]
		result = re.match("location:\s*(.*)$", result, re.IGNORECASE)
		if result and result.groups()[0]:
			result = result.groups()[0]
		else:
			result = url
	else:
		# Either the url is canonical or we are broken
		result = url

	# Cache the result
	if result is not None:
		state.set("canonical_url_%s" % hash_key, result)

	raise gen.Return(result)