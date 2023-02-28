import argparse

import logging

import json

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

import apache_beam as beam



class CustomParsing(beam.DoFn):
     def process(self, element:bytes):

        parsed = json.loads(element.decode("utf-8"))

        parsed["Age"]=int(parsed["Age"])

        yield parsed



def run():



    parser = argparse.ArgumentParser()

    parser.add_argument('--input',dest='input',

            required=False,

            help='Input file to read. This can be a local file or '

                 'a file in a Google Storage Bucket.',

            default='projects/mlconsole-poc/topics/srh' )

    parser.add_argument('--output',


                        dest='output',

                        required=False,

                        help='Output BQ table to write results to.',

                        default='sarthak.p2')

    parser.add_argument('--project', dest='project', required=False, help='Project name', default='mlconsole-poc',

                        action="store")

    parser.add_argument('--runner', dest='runner', required=False, help='Runner Name', default='DataflowRunner',

                        action="store")

    parser.add_argument('--jobname', dest='job_name', required=False, help='jobName', default='job3333ptob',

                        action="store")

    parser.add_argument('--staging_location', dest='staging_location', required=False, help='staging_location',

                        default='gs://sarthak11/staging')

    parser.add_argument('--region', dest='region', required=False, help='Region', default='us-east1',

                        action="store")

    parser.add_argument('--temp_location', dest='temp_location', required=False, help='temp location',

                        default='gs://sarthak11/temp')





    args = parser.parse_args()



    pipeline_options = {

        'project': args.project,

        'staging_location': args.staging_location,

        'runner': args.runner,

        'job_name': args.job_name,

        'region': args.region,

        'output': args.output,

        'input': args.input,

        'temp_location': args.temp_location,

        }

    print(pipeline_options)

    pipeline_options_val = PipelineOptions.from_dictionary(pipeline_options)

    p = beam.Pipeline(options=pipeline_options_val)

    pipeline_options_val.view_as(StandardOptions).streaming = True

    dataset_id = 'sarthak'  # replace with your dataset ID

    table_id = 'p2'  # replace with your table ID

    table_schema = ('Age:INTEGER,Name:STRING,Date:STRING,City:STRING')




    (p | 'Read from file'>>beam.io.ReadFromPubSub(pipeline_options["input"])

       | "Covert to int" >> beam.ParDo(CustomParsing())

      | 'Write to BigQuery' >> beam.io.WriteToBigQuery(

                   table=table_id,

                    dataset=dataset_id,

                    project=pipeline_options["project"],

                    schema=table_schema,

                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,

                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

                    ))

    p.run().wait_until_finish()



if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)

    run()
