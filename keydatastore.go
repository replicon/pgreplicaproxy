package main

type backendKeyDataMessage struct {
	processId int32
	secretKey int32
}

type registerBackendKeyMessage struct {
	key     backendKeyDataMessage
	backend string
}

type deregisterBackendKeyMessage struct {
	key backendKeyDataMessage
}

type getBackendForBackendKeyMessage struct {
	key        backendKeyDataMessage
	returnChan chan *string
}

var registerBackendKeyChan chan registerBackendKeyMessage = make(chan registerBackendKeyMessage)
var deregisterBackedKeyChan chan deregisterBackendKeyMessage = make(chan deregisterBackendKeyMessage)
var getBackendForBackendKeyChan chan getBackendForBackendKeyMessage = make(chan getBackendForBackendKeyMessage)

func registerBackendKey(backendKeyData backendKeyDataMessage, backend string) {
	registerBackendKeyChan <- registerBackendKeyMessage{
		backendKeyData,
		backend,
	}
}

func deregisterBackedKey(backendKeyData backendKeyDataMessage) {
	deregisterBackedKeyChan <- deregisterBackendKeyMessage{backendKeyData}
}

func getBackendForBackendKeyData(backendKeyData backendKeyDataMessage) *string {
	msg := getBackendForBackendKeyMessage{
		backendKeyData,
		make(chan *string),
	}
	getBackendForBackendKeyChan <- msg
	return <-msg.returnChan
}

func manageBackendKeyDataStorage() {
	store := make(map[backendKeyDataMessage]string)
	for {
		select {
		case register := <-registerBackendKeyChan:
			store[register.key] = register.backend

		case deregister := <-deregisterBackedKeyChan:
			delete(store, deregister.key)

		case get := <-getBackendForBackendKeyChan:
			v, ok := store[get.key]
			if ok {
				get.returnChan <- &v
			} else {
				get.returnChan <- nil
			}
		}
	}
}
