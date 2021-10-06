#include "h5dbg.h"
#include <tid/tid.h>



std::string tools::h5dbg::get_hid_string_details(const hid_t & id){
    auto        t_scope  = tid::tic_scope(__FUNCTION__);
    auto hid_type = H5Iget_type(id);
    std::string msg = fmt::format("id {}: ",id);
    switch(hid_type){
        case H5I_type_t::H5I_DATASET : {
            h5pp::hid::h5d hid = id;
            msg.append("[DATASET]: ");
            msg.append(h5pp::hdf5::getName(hid));
            break;
        }
        case H5I_type_t::H5I_ATTR : {
            h5pp::hid::h5a hid = id;
            msg.append("[ATTRIBUTE]: ");
            msg.append(h5pp::hdf5::getName(hid));
            break;
        }
        case H5I_type_t::H5I_DATATYPE : {
            h5pp::hid::h5t hid = id;
            msg.append("[DATATYPE]: ");
            msg.append(h5pp::hdf5::getName(hid));
            break;
        }
        case H5I_type_t::H5I_DATASPACE : {
            h5pp::hid::h5s hid = id;
            msg.append("[DATASPACE]: ");
            msg.append(h5pp::hdf5::getName(hid));
            break;
        }
        case H5I_type_t::H5I_GROUP : {
            h5pp::hid::h5g hid = id;
            msg.append("[GROUP]: ");
            msg.append(h5pp::hdf5::getName(hid));
            break;
        }
        case H5I_type_t::H5I_FILE : {
            h5pp::hid::h5f hid = id;
            msg.append("[FILE]: ");
            msg.append(h5pp::hdf5::getName(hid));
            break;
        }
        case H5I_type_t::H5I_BADID : {
            msg.append("[BADID] ");
            break;
        }
        default:
            msg.append("[UNKNOWN]");
    }
    return msg;
}
