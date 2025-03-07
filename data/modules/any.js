var InitModule = function Any(ctx, logger, nk, initializer) {
	initializer.registerBeforeAny(beforeAny)
	initializer.registerAfterAny(afterAny)
	initializer.registerPeerEvent(eventPeer)
	logger.info('javascript logic loaded')
}

function beforeAny(context, logger, nk, payload) {
	logger.debug("beforeAny: %v", payload)
	var m = nk.invokeMS("testjs", "say_hello", {hk:"tes"}, {ui:["ddd", "dddd"]}, {u:"ddd"}, "uu888889999")
	logger.debug('res:%v', m)
	return payload
}

function afterAny(context, logger, nk, payload) {
	logger.debug("afterAny: %v", payload)
	return payload
}

function eventPeer(context, logger, nk, payload) {
	logger.debug("context: %v", context)
	logger.debug("logger: %v", logger)
	logger.debug("nk: %v", nk)
	logger.debug("payload: %v", payload)
  }