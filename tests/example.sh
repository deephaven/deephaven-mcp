dhcli catalog namespaces entuser -o json-pretty

dhcli catalog tables entuser --filter 'Namespace = `DbInternal`' -o json-pretty   