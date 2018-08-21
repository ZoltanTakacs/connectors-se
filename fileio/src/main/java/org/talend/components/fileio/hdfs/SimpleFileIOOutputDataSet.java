package org.talend.components.fileio.hdfs;

import static org.talend.sdk.component.api.component.Icon.IconType.FILE_HDFS_O;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@Icon(FILE_HDFS_O)
@EqualsAndHashCode(callSuper = true)
@DataSet("SimpleFileIOOutputDataSet")
@Documentation("Dataset of a HDFS source.")
@OptionsOrder({ "datastore", "format", "path", "recordDelimiter", "specificRecordDelimiter", "fieldDelimiter",
        "specificFieldDelimiter", "overwrite", "mergeOutput", "limit" })
public class SimpleFileIOOutputDataSet extends SimpleFileIODataSet {

    @Option
    @Documentation("Should overwrite data.")
    private boolean overwrite;

    @Option
    @Documentation("Should merge data.")
    private boolean mergeOutput;
}
