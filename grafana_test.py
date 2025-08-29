from grafana_api.grafana_face import GrafanaFace

grafana_connection = GrafanaFace(auth=('admin', 'admin'), host='localhost:3000')

print(grafana_connection.datasource.delete_datasource_by_name('prometheus'))