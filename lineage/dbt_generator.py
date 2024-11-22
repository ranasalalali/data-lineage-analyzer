import os

def create_dbt_sources_and_models(initial_sources, lineage_graph, models_dir, impacted_tables):
    os.makedirs(models_dir, exist_ok=True)
    sources_yml_path = os.path.join(models_dir, 'sources.yml')

    database = 'raw'
    schema_name = 'silver'

    # Generate the sources.yml file for initial source tables
    with open(sources_yml_path, 'w') as f:
        f.write("version: 2\n")
        f.write("sources:\n")
        f.write(" - name: silver\n")
        f.write(f"   database: {database}\n")
        f.write(f"   schema: {schema_name}\n")
        f.write("   tables:\n")
        for source in initial_sources:
            f.write(f"   - name: {source.lower()}\n")
    print(f"Generated sources file: {sources_yml_path}")

    # Generate models for non-source tables
    for node, connections in lineage_graph.graph.items():
        node_parts = node.split('.')
        node_name = node_parts[1] if len(node_parts) > 1 else node_parts[0]

        # Check if the node is not a direct source
        if node_name.lower() not in (s.lower() for s in initial_sources):
            model_filename = os.path.join(models_dir, f"{node_name}.sql")
            with open(model_filename, 'w') as f:
                parent_refs = []
                # Highlight impacted tables in red
                color = ", docs={'node_color': 'red'}" if node_name.lower() in (t.lower() for t in impacted_tables) else ''
                for parent in connections['parents']:
                    parent_parts = parent.split('.')
                    parent_name = parent_parts[1] if len(parent_parts) > 1 else parent_parts[0]

                    if parent_name.lower() in (s.lower() for s in initial_sources):
                        parent_refs.append(f"{{{{ source('silver', '{parent_name.lower()}') }}}}")
                    else:
                        parent_refs.append(f"{{{{ ref('{parent_name.lower()}') }}}}")

                if len(parent_refs) > 1:
                    f.write(f"{{{{ config(materialized='view'{color}) }}}}\n\n")
                    f.write("select *\nfrom (\n")
                    for i, parent_ref in enumerate(parent_refs):
                        if i > 0:
                            f.write("join\n")
                        f.write(f"\t{parent_ref} as t{i}\n")
                    f.write(")\n")
                elif len(parent_refs) == 1:
                    f.write(f"{{{{ config(materialized='view'{color}) }}}}\n\n")
                    f.write("select *\n")
                    f.write(f"from {parent_refs[0]}\n")
            print(f"Generated model file: {model_filename}")