class GeneralError extends Error {
	constructor({ message, extraDetails }) {
		super();
		this.devMessage = message;
		this.message = message;
		this.extraDetails = extraDetails;
		this.stackTrace = this.stack;
		switch (this.constructor.name) {
			case 'ConnectionError': {
				this.message = 'Connection to CouchBase failed!';
				break;
			}
			default: {
				this.message = 'Unrecognized error.';
			}
		}
	}
}

class ConnectionError extends GeneralError {}

module.exports = {
	GeneralError,
	ConnectionError
};
