FROM mcr.microsoft.com/dotnet/core/aspnet:3.1 AS base
WORKDIR /app
EXPOSE 80
ENV DD_AGENT_VERSION 1.16.0
ENV CORECLR_ENABLE_PROFILING=1
ENV CORECLR_PROFILER={846F5F1C-F9AE-4B07-969E-05C26BC060D8}
ENV CORECLR_PROFILER_PATH=/opt/datadog/Datadog.Trace.ClrProfiler.Native.so
ENV DD_INTEGRATIONS=/opt/datadog/integrations.json
ENV DD_DOTNET_TRACER_HOME=/opt/datadog
ENV DD_LOGS_INJECTION=true
ENV DD_ADONET_ENABLED=false
RUN curl -LO https://github.com/DataDog/dd-trace-dotnet/releases/download/v${DD_AGENT_VERSION}/datadog-dotnet-apm_${DD_AGENT_VERSION}_amd64.deb
RUN dpkg -i ./datadog-dotnet-apm_${DD_AGENT_VERSION}_amd64.deb


FROM mcr.microsoft.com/dotnet/core/sdk:3.1.201 AS build
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF \
&& echo "deb http://download.mono-project.com/repo/debian stretch main" | tee /etc/apt/sources.list.d/mono-official.list \
&& apt-get update && apt-get install -y mono-devel default-jre build-essential libssl-dev libsasl2-2 libsasl2-dev libsasl2-modules-gssapi-mit wget unzip

# Build librdkafka
ENV LIBRDKAFKA_VER=1.4.0
RUN curl -k -L -s https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VER}.zip -o ./librdkafka.zip

RUN ls -l && cd / && unzip librdkafka.zip && \
cd librdkafka-${LIBRDKAFKA_VER} && \
./configure && \
make && \
make install

WORKDIR /src
COPY . .
RUN dotnet tool restore
ENV PATH="/root/.dotnet/tools:${PATH}"

WORKDIR /src/src/RabbitMqKafkaConnector
RUN dotnet paket restore
RUN dotnet restore -nowarn:msb3202,nu1503
RUN dotnet build -c Release -o /app


FROM build AS publish
RUN dotnet publish --no-restore -c Release -o /app

FROM base AS final
# Install runtime dependencies for kerberos
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install krb5-user kstart \
libsasl2-2 libsasl2-modules-gssapi-mit libsasl2-modules \
&& apt-get autoremove

WORKDIR /kafka

RUN apt-get -y update && apt-get -y install \
        build-essential \
        krb5-user \
        libsasl2-modules-gssapi-mit \
        libsasl2-dev \
        git \
        && git init \
        && git clone https://github.com/edenhill/librdkafka.git 

WORKDIR /kafka/librdkafka

RUN ./configure --install-deps --prefix=/usr \
        && make -j && make install

WORKDIR /app

RUN rm -f /app/runtimes/linux-x64/native/librdkafka.so
COPY --from=build /usr/local/lib/librdkafka*.so* /app/runtimes/linux-x64/native/
COPY --from=publish /app .

ENTRYPOINT ["dotnet", "RabbitMqKafkaConnector.dll"]